package sync

import (
	"crypto/rand"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/plugins/sync/events"
	"github.com/pocketbase/pocketbase/plugins/sync/nats"
	"github.com/pocketbase/pocketbase/plugins/sync/snapshot"
	syncpkg "github.com/pocketbase/pocketbase/plugins/sync/sync"
)

// Config defines the config options of the sync plugin.
type Config struct {
	// Empty for now, following plugin pattern
}

// MustRegister registers the sync plugin to the provided app instance
// and panics if it fails.
func MustRegister(app core.App, config Config) {
	if err := Register(app, config); err != nil {
		panic(err)
	}
}

// Register registers the sync plugin to the provided app instance.
func Register(app core.App, config Config) error {
	p := &plugin{
		app:    app,
		config: config,
	}

	// Hook into app lifecycle
	app.OnBootstrap().BindFunc(p.onBootstrap)
	app.OnServe().BindFunc(p.onServe)
	app.OnSettingsUpdateRequest().BindFunc(p.onSettingsUpdate)

	return nil
}

type plugin struct {
	app              core.App
	config           Config
	mu               sync.RWMutex
	embeddedNATS     *nats.EmbeddedServer
	publisher        *nats.Publisher
	subscriber       *nats.Subscriber
	processor        *events.Processor
	realtimeManager  *syncpkg.RealtimeManager
	snapshotManager  *snapshot.Manager
	instanceID       string
	natsURL          string
	snapshotInterval int64
	syncing          bool
	syncingSchema    bool
	hooksRegistered  bool
	stopChan         chan struct{}
}

func (p *plugin) onBootstrap(e *core.BootstrapEvent) error {
	settings := p.app.Settings()

	// Check if sync is enabled
	if !settings.Sync.Enabled {
		return e.Next()
	}

	// Auto-generate instance ID if empty
	if settings.Sync.InstanceID == "" {
		newID := generateUUID()
		settings.Sync.InstanceID = newID
		if err := p.app.Save(settings); err != nil {
			log.Printf("Warning: Failed to save auto-generated instance ID: %v", err)
		} else {
			log.Printf("Auto-generated instance ID: %s", newID)
		}
	}

	// Set default natsURL if empty
	if settings.Sync.NatsURL == "" {
		settings.Sync.NatsURL = "0.0.0.0:4222"
		if err := p.app.Save(settings); err != nil {
			log.Printf("Warning: Failed to save default NATS URL: %v", err)
		}
	}

	// Set default snapshot interval if empty
	if settings.Sync.SnapshotInterval == 0 {
		settings.Sync.SnapshotInterval = 24
		if err := p.app.Save(settings); err != nil {
			log.Printf("Warning: Failed to save default snapshot interval: %v", err)
		}
	}

	return e.Next()
}

func (p *plugin) onServe(e *core.ServeEvent) error {
	settings := p.app.Settings()

	// Add manual snapshot trigger endpoint (only if sync is enabled)
	if settings.Sync.Enabled {
		apiGroup := e.Router.Group("/api")
		syncGroup := apiGroup.Group("/sync")
		syncGroup.POST("/snapshot", p.handleManualSnapshot)
	}

	// Check if sync is enabled
	if !settings.Sync.Enabled {
		return e.Next()
	}

	// Start sync
	if err := p.startSync(); err != nil {
		return fmt.Errorf("failed to start sync: %w", err)
	}

	return e.Next()
}

// handleManualSnapshot handles manual snapshot creation requests
func (p *plugin) handleManualSnapshot(e *core.RequestEvent) error {
	// Require admin authentication
	if e.Auth == nil || e.Auth.IsAdmin() == false {
		return e.UnauthorizedError("Admin authentication required", nil)
	}

	// Check if sync is enabled
	settings := p.app.Settings()
	if !settings.Sync.Enabled {
		return e.BadRequestError("Sync is not enabled", nil)
	}

	// Check if snapshot manager exists
	p.mu.RLock()
	snapshotManager := p.snapshotManager
	p.mu.RUnlock()

	if snapshotManager == nil {
		return e.InternalServerError("Snapshot manager not initialized", nil)
	}

	// Create snapshot
	snap, err := snapshotManager.CreateSnapshot()
	if err != nil {
		return e.InternalServerError("Failed to create snapshot", err)
	}

	// Return snapshot info
	return e.JSON(200, map[string]interface{}{
		"success":       true,
		"timestamp":     snap.Timestamp,
		"instanceID":    snap.InstanceID,
		"collections":   len(snap.Collections),
		"eventSequence": snap.EventSequence,
	})
}

// startSync initializes and starts the sync infrastructure
// If settings is provided, it will be used instead of reading from app.Settings()
func (p *plugin) startSync(settings ...*core.Settings) error {
	var syncSettings *core.Settings
	if len(settings) > 0 && settings[0] != nil {
		syncSettings = settings[0]
	} else {
		syncSettings = p.app.Settings()
	}

	// Check if sync is enabled
	if !syncSettings.Sync.Enabled {
		return nil
	}

	p.instanceID = syncSettings.Sync.InstanceID
	p.natsURL = syncSettings.Sync.NatsURL
	p.snapshotInterval = syncSettings.Sync.SnapshotInterval

	// Ensure system collections exist
	if err := syncpkg.EnsurePBSyncCollection(p.app.(*pocketbase.PocketBase)); err != nil {
		return fmt.Errorf("failed to ensure _pbSync collection: %w", err)
	}

	if err := syncpkg.EnsurePBRealtimeSubscriptionsCollection(p.app.(*pocketbase.PocketBase)); err != nil {
		log.Printf("Warning: Failed to ensure _pbRealtimeSubscriptions collection: %v", err)
	}

	// Start embedded NATS server
	storeDir := filepath.Join(p.app.DataDir(), "nats_data")
	embeddedNATS, err := nats.NewEmbeddedServer(p.natsURL, true, storeDir)
	if err != nil {
		return fmt.Errorf("failed to start embedded NATS server: %w", err)
	}
	p.embeddedNATS = embeddedNATS

	// Create NATS publisher and subscriber
	streamName := "pocketbase-sync"
	publisher, err := nats.NewPublisher(embeddedNATS, streamName)
	if err != nil {
		return fmt.Errorf("failed to create NATS publisher: %w", err)
	}
	p.publisher = publisher

	subscriber, err := nats.NewSubscriber(embeddedNATS, streamName, p.instanceID)
	if err != nil {
		return fmt.Errorf("failed to create NATS subscriber: %w", err)
	}
	p.subscriber = subscriber

	// Create snapshot manager
	snapshotManager, err := snapshot.NewManager(p.app.(*pocketbase.PocketBase), embeddedNATS.JetStream(), streamName, p.instanceID)
	if err != nil {
		return fmt.Errorf("failed to create snapshot manager: %w", err)
	}
	p.snapshotManager = snapshotManager

	// Don't create self record yet - wait until after discovery completes
	// This makes the process more organic: instances discover each other, then announce themselves

	// Create event processor
	processor := events.NewProcessor(p.app.(*pocketbase.PocketBase), p.instanceID)
	p.processor = processor

	// Register hooks for database changes (only once)
	p.mu.Lock()
	if !p.hooksRegistered {
		p.registerHooks()
		p.hooksRegistered = true
	}
	p.mu.Unlock()

	// Connect to existing instances
	allInstances, err := syncpkg.GetAllInstances(p.app.(*pocketbase.PocketBase))
	if err != nil {
		log.Printf("Warning: failed to get existing instances: %v", err)
		allInstances = []*core.Record{}
	}

	// Connect to other instances and apply snapshots
	if len(allInstances) > 1 {
		p.connectToInstances(allInstances, streamName)
	}

	// Start subscriber
	if err := subscriber.Start(); err != nil {
		return fmt.Errorf("failed to start NATS subscriber: %w", err)
	}

	// Start event processing
	p.stopChan = make(chan struct{})
	go p.processEvents()

	// Start snapshot job
	go p.startSnapshotJob()

	// Create realtime manager
	realtimeManager, err := syncpkg.NewRealtimeManager(p.app.(*pocketbase.PocketBase), p.instanceID)
	if err != nil {
		log.Printf("Warning: Failed to create realtime manager: %v", err)
	} else {
		p.realtimeManager = realtimeManager
		// Set publisher functions
		realtimeManager.SetPublishers(
			func(resourceID string, activeConnections int, leaseExpires time.Time) error {
				event := events.NewRealtimeSubscribeEvent(p.instanceID, resourceID, activeConnections, leaseExpires)
				return publisher.Publish(event)
			},
			func(resourceID string) error {
				event := events.NewRealtimeUnsubscribeEvent(p.instanceID, resourceID)
				return publisher.Publish(event)
			},
			func(resourceID string, activeConnections int, leaseExpires time.Time) error {
				event := events.NewRealtimeHeartbeatEvent(p.instanceID, resourceID, activeConnections, leaseExpires)
				return publisher.Publish(event)
			},
		)
	}

	log.Printf("Sync plugin initialized successfully (instance: %s)", p.instanceID)

	return nil
}

func (p *plugin) onSettingsUpdate(e *core.SettingsUpdateRequestEvent) error {
	oldSettings := e.OldSettings
	newSettings := e.NewSettings

	// Check if sync was disabled
	if oldSettings.Sync.Enabled && !newSettings.Sync.Enabled {
		// Stop sync
		p.stopSync()
		return e.Next()
	}

	// Check if sync was enabled
	if !oldSettings.Sync.Enabled && newSettings.Sync.Enabled {
		// Validate required fields before starting NATS and creating collections
		// This allows the Svelte form validation to catch missing fields first
		if newSettings.Sync.InstanceID == "" {
			log.Printf("Cannot start sync: instanceID is required")
			// Don't fail the settings update, but don't start sync either
			return e.Next()
		}
		if newSettings.Sync.NatsURL == "" {
			log.Printf("Cannot start sync: natsURL is required")
			// Don't fail the settings update, but don't start sync either
			return e.Next()
		}

		// Ensure system collections exist before starting sync
		app := p.app.(*pocketbase.PocketBase)
		if err := syncpkg.EnsurePBSyncCollection(app); err != nil {
			log.Printf("Error ensuring _pbSync collection: %v", err)
			// Don't fail the settings update, but log the error
		}
		if err := syncpkg.EnsurePBRealtimeSubscriptionsCollection(app); err != nil {
			log.Printf("Warning: Failed to ensure _pbRealtimeSubscriptions collection: %v", err)
		}

		// Start sync immediately with new settings (before they're saved)
		log.Printf("Sync enabled, starting NATS server...")
		if err := p.startSync(newSettings); err != nil {
			log.Printf("Error starting sync: %v", err)
			// Don't fail the settings update, but log the error
		}
		return e.Next()
	}

	// Check if sync is enabled and settings changed that require restart
	if newSettings.Sync.Enabled {
		needsRestart := false

		// Check if NATS URL changed
		if oldSettings.Sync.NatsURL != newSettings.Sync.NatsURL {
			log.Printf("NATS URL changed, restarting NATS server...")
			needsRestart = true
		}

		// Check if instance ID changed
		if oldSettings.Sync.InstanceID != newSettings.Sync.InstanceID {
			log.Printf("Instance ID changed, restarting...")
			needsRestart = true
		}

		// Check if snapshot interval changed
		if oldSettings.Sync.SnapshotInterval != newSettings.Sync.SnapshotInterval {
			// Update the interval (the job will pick it up on next tick)
			p.mu.Lock()
			p.snapshotInterval = newSettings.Sync.SnapshotInterval
			p.mu.Unlock()
		}

		if needsRestart {
			// Validate required fields before restarting NATS and creating collections
			if newSettings.Sync.InstanceID == "" {
				log.Printf("Cannot restart sync: instanceID is required")
				// Don't fail the settings update, but don't restart sync either
				return e.Next()
			}
			if newSettings.Sync.NatsURL == "" {
				log.Printf("Cannot restart sync: natsURL is required")
				// Don't fail the settings update, but don't restart sync either
				return e.Next()
			}

			// Ensure system collections exist before restarting sync
			app := p.app.(*pocketbase.PocketBase)
			if err := syncpkg.EnsurePBSyncCollection(app); err != nil {
				log.Printf("Error ensuring _pbSync collection: %v", err)
			}
			if err := syncpkg.EnsurePBRealtimeSubscriptionsCollection(app); err != nil {
				log.Printf("Warning: Failed to ensure _pbRealtimeSubscriptions collection: %v", err)
			}

			p.stopSync()
			// Use new settings for restart (before they're saved)
			if err := p.startSync(newSettings); err != nil {
				log.Printf("Error restarting sync: %v", err)
				// Don't fail the settings update, but log the error
			}
		}
	}

	return e.Next()
}

func (p *plugin) registerHooks() {
	app := p.app.(*pocketbase.PocketBase)

	// Record hooks
	app.OnRecordAfterCreateSuccess().BindFunc(func(e *core.RecordEvent) error {
		// Skip system collections that don't need syncing
		if e.Record.Collection().Name == syncpkg.PBRealtimeSubscriptionsCollectionName {
			return nil
		}

		p.mu.RLock()
		syncing := p.syncing
		p.mu.RUnlock()
		if syncing {
			return nil
		}

		// Handle _pbSync collection: trigger discovery for new instances, then publish normally
		if e.Record.Collection().Name == syncpkg.PBSyncCollectionName {
			return p.handlePBSyncCreate(e)
		}

		return p.handleCreate(e)
	})

	app.OnRecordAfterUpdateSuccess().BindFunc(func(e *core.RecordEvent) error {
		// Skip system collections that don't need syncing
		if e.Record.Collection().Name == syncpkg.PBRealtimeSubscriptionsCollectionName {
			return nil
		}

		p.mu.RLock()
		syncing := p.syncing
		p.mu.RUnlock()
		if syncing {
			return nil
		}

		// Handle _pbSync collection: trigger discovery for updated instances, then publish normally
		if e.Record.Collection().Name == syncpkg.PBSyncCollectionName {
			return p.handlePBSyncUpdate(e)
		}

		return p.handleUpdate(e)
	})

	app.OnRecordAfterDeleteSuccess().BindFunc(func(e *core.RecordEvent) error {
		// Skip system collections that don't need syncing
		if e.Record.Collection().Name == syncpkg.PBRealtimeSubscriptionsCollectionName {
			return nil
		}

		p.mu.RLock()
		syncing := p.syncing
		p.mu.RUnlock()
		if syncing {
			return nil
		}

		// Handle _pbSync collection: remove connections for deleted instances, then publish normally
		if e.Record.Collection().Name == syncpkg.PBSyncCollectionName {
			return p.handlePBSyncDelete(e)
		}

		return p.handleDelete(e)
	})

	// Collection hooks
	app.OnCollectionAfterCreateSuccess().BindFunc(func(e *core.CollectionEvent) error {
		p.mu.RLock()
		syncing := p.syncingSchema
		p.mu.RUnlock()
		if syncing {
			return nil
		}
		return p.handleCollectionCreate(e)
	})

	app.OnCollectionAfterUpdateSuccess().BindFunc(func(e *core.CollectionEvent) error {
		p.mu.RLock()
		syncing := p.syncingSchema
		p.mu.RUnlock()
		if syncing {
			return nil
		}
		return p.handleCollectionUpdate(e)
	})

	app.OnCollectionAfterDeleteSuccess().BindFunc(func(e *core.CollectionEvent) error {
		p.mu.RLock()
		syncing := p.syncingSchema
		p.mu.RUnlock()
		if syncing {
			return nil
		}
		return p.handleCollectionDelete(e)
	})

	// Realtime hooks - integrate RealtimeManager with realtime API
	app.OnRealtimeSubscribeRequest().BindFunc(func(e *core.RealtimeSubscribeRequestEvent) error {
		// Only process if sync is enabled and realtime manager exists
		p.mu.RLock()
		realtimeManager := p.realtimeManager
		p.mu.RUnlock()

		if realtimeManager == nil {
			return e.Next()
		}

		// Get previous subscriptions before they're updated
		previousSubs := e.Client.Subscriptions()
		previousSubsMap := make(map[string]bool)
		for _, sub := range previousSubs {
			previousSubsMap[sub] = true
		}

		// Process the subscription update first
		if err := e.Next(); err != nil {
			return err
		}

		// After subscriptions are updated, process them
		newSubs := e.Client.Subscriptions()
		newSubsMap := make(map[string]bool)
		for _, sub := range newSubs {
			newSubsMap[sub] = true
		}

		// Handle new subscriptions
		for _, sub := range newSubs {
			if !previousSubsMap[sub] {
				// New subscription - register it
				if err := realtimeManager.HandleSubscribe(sub); err != nil {
					log.Printf("Warning: Failed to handle realtime subscribe for %s: %v", sub, err)
				}
			}
		}

		// Handle removed subscriptions
		for _, sub := range previousSubs {
			if !newSubsMap[sub] {
				// Subscription removed - unregister it
				if err := realtimeManager.HandleUnsubscribe(sub); err != nil {
					log.Printf("Warning: Failed to handle realtime unsubscribe for %s: %v", sub, err)
				}
			}
		}

		return nil
	})
}

// Helper functions

func (p *plugin) handleCreate(e *core.RecordEvent) error {
	p.mu.RLock()
	syncing := p.syncing
	p.mu.RUnlock()
	if syncing {
		return nil
	}

	recordData := p.getAllRecordFields(e.Record)
	event := events.NewCreateEvent(
		p.instanceID,
		e.Record.Collection().Name,
		e.Record.Id,
		recordData,
	)
	p.publisher.PublishAsync(event)
	return nil
}

func (p *plugin) handleUpdate(e *core.RecordEvent) error {
	p.mu.RLock()
	syncing := p.syncing
	p.mu.RUnlock()
	if syncing {
		return nil
	}

	recordData := p.getAllRecordFields(e.Record)
	event := events.NewUpdateEvent(
		p.instanceID,
		e.Record.Collection().Name,
		e.Record.Id,
		recordData,
	)
	p.publisher.PublishAsync(event)
	return nil
}

func (p *plugin) handleDelete(e *core.RecordEvent) error {
	p.mu.RLock()
	syncing := p.syncing
	p.mu.RUnlock()
	if syncing {
		return nil
	}

	event := events.NewDeleteEvent(
		p.instanceID,
		e.Record.Collection().Name,
		e.Record.Id,
	)
	p.publisher.PublishAsync(event)
	return nil
}

func (p *plugin) handlePBSyncCreate(e *core.RecordEvent) error {
	instanceID := e.Record.GetString("instanceID")
	natsAddress := e.Record.GetString("natsAddress")

	// Skip discovery for our own instance
	if instanceID == p.instanceID {
		return p.handleCreate(e)
	}

	// Trigger discovery if it's a different instance
	if natsAddress != "" {
		// Trigger discovery in background (don't block)
		go func() {
			if err := p.discoverInstance(instanceID, natsAddress); err != nil {
				log.Printf("Warning: discovery failed for instance %s: %v", instanceID, err)
			}
		}()
	}

	// Still publish the event (but discovery won't trigger again when it's synced)
	return p.handleCreate(e)
}

func (p *plugin) handlePBSyncUpdate(e *core.RecordEvent) error {
	instanceID := e.Record.GetString("instanceID")
	natsAddress := e.Record.GetString("natsAddress")

	// Skip discovery for our own instance
	if instanceID == p.instanceID {
		return p.handleUpdate(e)
	}

	// Trigger discovery if it's a different instance
	if natsAddress != "" {
		// Trigger discovery in background (don't block)
		go func() {
			if err := p.discoverInstance(instanceID, natsAddress); err != nil {
				log.Printf("Warning: discovery failed for instance %s: %v", instanceID, err)
			}
		}()
	}

	// Still publish the event
	return p.handleUpdate(e)
}

func (p *plugin) handlePBSyncDelete(e *core.RecordEvent) error {
	instanceID := e.Record.GetString("instanceID")

	// Skip removal for our own instance
	if instanceID == p.instanceID {
		return p.handleDelete(e)
	}

	// Remove connections in background (don't block)
	go func() {
		if err := p.publisher.RemoveConnection(instanceID); err != nil {
			log.Printf("Warning: failed to remove publisher connection: %v", err)
		}
		if err := p.subscriber.RemoveConnection(instanceID); err != nil {
			log.Printf("Warning: failed to remove subscriber connection: %v", err)
		}
	}()

	// Still publish the event
	return p.handleDelete(e)
}

func (p *plugin) handleCollectionCreate(e *core.CollectionEvent) error {
	if e.Collection.Name == syncpkg.PBSyncCollectionName {
		return nil
	}

	p.mu.RLock()
	syncing := p.syncingSchema
	p.mu.RUnlock()
	if syncing {
		return nil
	}

	schemaData := p.exportCollectionSchema(e.Collection)
	event := events.NewCollectionCreateEvent(p.instanceID, e.Collection.Name, schemaData)
	p.publisher.PublishAsync(event)
	return nil
}

func (p *plugin) handleCollectionUpdate(e *core.CollectionEvent) error {
	if e.Collection.Name == syncpkg.PBSyncCollectionName {
		return nil
	}

	p.mu.RLock()
	syncing := p.syncingSchema
	p.mu.RUnlock()
	if syncing {
		return nil
	}

	schemaData := p.exportCollectionSchema(e.Collection)
	event := events.NewCollectionUpdateEvent(p.instanceID, e.Collection.Name, schemaData)
	p.publisher.PublishAsync(event)
	return nil
}

func (p *plugin) handleCollectionDelete(e *core.CollectionEvent) error {
	if e.Collection.Name == syncpkg.PBSyncCollectionName {
		return nil
	}

	p.mu.RLock()
	syncing := p.syncingSchema
	p.mu.RUnlock()
	if syncing {
		return nil
	}

	event := events.NewCollectionDeleteEvent(p.instanceID, e.Collection.Name)
	p.publisher.PublishAsync(event)
	return nil
}

func (p *plugin) getAllRecordFields(record *core.Record) map[string]any {
	collection := record.Collection()
	recordData := make(map[string]any)

	var row dbx.NullStringMap
	err := p.app.(*pocketbase.PocketBase).DB().NewQuery("SELECT * FROM " + collection.Name + " WHERE id = {:id}").
		Bind(dbx.Params{"id": record.Id}).
		One(&row)

	if err == nil {
		for key, value := range row {
			if value.Valid {
				recordData[key] = value.String
			}
		}
	} else {
		recordData = record.FieldsData()
		if collection.Type == core.CollectionTypeAuth || collection.Name == "_superusers" {
			var pwdHash dbx.NullStringMap
			err2 := p.app.(*pocketbase.PocketBase).DB().NewQuery("SELECT password FROM " + collection.Name + " WHERE id = {:id}").
				Bind(dbx.Params{"id": record.Id}).
				One(&pwdHash)
			if err2 == nil && pwdHash["password"].Valid {
				recordData["password"] = pwdHash["password"].String
			}
		}
	}

	if record.Id != "" {
		recordData["id"] = record.Id
	}

	return recordData
}

func (p *plugin) exportCollectionSchema(collection *core.Collection) map[string]any {
	colMap := make(map[string]any)
	colMap["id"] = collection.Id
	colMap["name"] = collection.Name
	colMap["type"] = string(collection.Type)
	colMap["system"] = collection.System
	if collection.ListRule != nil {
		colMap["listRule"] = *collection.ListRule
	}
	if collection.ViewRule != nil {
		colMap["viewRule"] = *collection.ViewRule
	}
	if collection.CreateRule != nil {
		colMap["createRule"] = *collection.CreateRule
	}
	if collection.UpdateRule != nil {
		colMap["updateRule"] = *collection.UpdateRule
	}
	if collection.DeleteRule != nil {
		colMap["deleteRule"] = *collection.DeleteRule
	}

	// Export fields using snapshot manager's ExportField
	fields := make([]map[string]any, 0, len(collection.Fields))
	for _, field := range collection.Fields {
		fieldMap := p.snapshotManager.ExportField(field)
		// Convert map[string]interface{} to map[string]any
		fieldMapAny := make(map[string]any)
		for k, v := range fieldMap {
			fieldMapAny[k] = v
		}
		fields = append(fields, fieldMapAny)
	}
	colMap["fields"] = fields

	// Export indexes
	indexes := make([]string, 0, len(collection.Indexes))
	for _, idx := range collection.Indexes {
		indexes = append(indexes, idx)
	}
	colMap["indexes"] = indexes

	return colMap
}

func (p *plugin) processEvents() {
	for {
		select {
		case <-p.stopChan:
			return
		case event := <-p.subscriber.Events():
			if event == nil {
				continue
			}

			if event.InstanceID == p.instanceID {
				continue
			}

			// Handle realtime events
			if event.Type == events.EventTypeRealtimeSubscribe ||
				event.Type == events.EventTypeRealtimeUnsubscribe ||
				event.Type == events.EventTypeRealtimeHeartbeat {
				if p.realtimeManager != nil {
					if err := p.realtimeManager.ProcessRealtimeEvent(string(event.Type), event.InstanceID, event.RealtimeData); err != nil {
						log.Printf("Error processing realtime event: %v", err)
					}
				}
				continue
			}

			// Set syncing flags
			p.mu.Lock()
			p.syncing = true
			if event.Type == events.EventTypeCollectionCreate ||
				event.Type == events.EventTypeCollectionUpdate ||
				event.Type == events.EventTypeCollectionDelete {
				p.syncingSchema = true
			}
			p.mu.Unlock()

			err := p.processor.Process(event)

			p.mu.Lock()
			p.syncing = false
			if event.Type == events.EventTypeCollectionCreate ||
				event.Type == events.EventTypeCollectionUpdate ||
				event.Type == events.EventTypeCollectionDelete {
				p.syncingSchema = false
			}
			p.mu.Unlock()

			if err != nil {
				log.Printf("Error processing event: %v", err)
			}
		}
	}
}

func (p *plugin) connectToInstances(allInstances []*core.Record, streamName string) {
	for _, inst := range allInstances {
		instID := inst.GetString("instanceID")
		if instID == p.instanceID {
			continue
		}

		natsAddress := inst.GetString("natsAddress")
		if natsAddress == "" {
			continue
		}

		instInfo := &syncpkg.InstanceInfo{
			InstanceID:  instID,
			NATSAddress: natsAddress,
		}

		conn, err := syncpkg.ConnectToNATS(instInfo)
		if err != nil {
			log.Printf("Warning: failed to connect to instance %s: %v", instID, err)
			continue
		}

		// Fetch snapshot before adding connections
		var startSeq uint64 = 0
		remoteJS, err := conn.JetStream()
		if err == nil {
			snap, err := snapshot.GetLatestSnapshotFromRemote(remoteJS, streamName)
			if err == nil && snap.InstanceID != p.instanceID {
				log.Printf("Found snapshot from instance %s, applying...", snap.InstanceID)
				p.mu.Lock()
				p.syncing = true
				p.syncingSchema = true
				p.mu.Unlock()
				if err := p.snapshotManager.ApplySnapshot(snap); err != nil {
					log.Printf("Warning: Failed to apply snapshot: %v", err)
				} else {
					if snap.EventSequence > 0 {
						startSeq = snap.EventSequence + 1
					}
				}
				p.mu.Lock()
				p.syncing = false
				p.syncingSchema = false
				p.mu.Unlock()
			}
		}

		if err := p.publisher.AddConnectionWithInstanceID(conn, instID); err != nil {
			log.Printf("Warning: failed to add publisher connection: %v", err)
			conn.Close()
			continue
		}

		if err := p.subscriber.AddConnectionWithInstanceID(conn, instID, startSeq); err != nil {
			log.Printf("Warning: failed to add subscriber connection: %v", err)
			continue
		}

		log.Printf("Connected to instance %s at %s", instID, natsAddress)

		// After connecting to an instance, ensure our self record exists and is published
		// This allows the connected instance to see us
		if err := p.ensureAndPublishSelfRecord(); err != nil {
			log.Printf("Warning: Failed to ensure/publish self record after connection: %v", err)
		}
	}

	// If we connected to any instances, ensure our self record is published
	// and publish all _pbSync records so all instances know about each other
	// If no instances exist yet, we'll publish it when the first discovery happens
	if len(allInstances) > 1 {
		if err := p.ensureAndPublishSelfRecord(); err != nil {
			log.Printf("Warning: Failed to ensure/publish self record: %v", err)
		}
		if err := p.publishAllPBSyncRecords(); err != nil {
			log.Printf("Warning: Failed to publish all _pbSync records: %v", err)
		}
	}
}

func (p *plugin) discoverInstance(instanceID, natsAddress string) error {
	if instanceID == p.instanceID {
		return nil
	}

	instInfo := &syncpkg.InstanceInfo{
		InstanceID:  instanceID,
		NATSAddress: natsAddress,
	}

	conn, err := syncpkg.ConnectToNATS(instInfo)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Apply snapshot before adding connections
	var startSeq uint64 = 0
	remoteJS, err := conn.JetStream()
	if err == nil {
		snap, err := snapshot.GetLatestSnapshotFromRemote(remoteJS, "pocketbase-sync")
		if err == nil && snap.InstanceID != p.instanceID {
			log.Printf("Found snapshot from instance %s, applying...", snap.InstanceID)
			p.mu.Lock()
			p.syncing = true
			p.syncingSchema = true
			p.mu.Unlock()
			if err := p.snapshotManager.ApplySnapshot(snap); err != nil {
				log.Printf("Warning: Failed to apply snapshot: %v", err)
			} else {
				if snap.EventSequence > 0 {
					startSeq = snap.EventSequence + 1
				}
			}
			p.mu.Lock()
			p.syncing = false
			p.syncingSchema = false
			p.mu.Unlock()
		}
	}

	if err := p.publisher.AddConnectionWithInstanceID(conn, instanceID); err != nil {
		conn.Close()
		return fmt.Errorf("failed to add publisher connection: %w", err)
	}

	if err := p.subscriber.AddConnectionWithInstanceID(conn, instanceID, startSeq); err != nil {
		return fmt.Errorf("failed to add subscriber connection: %w", err)
	}

	// After discovery completes, ensure our self record exists and is published
	// This allows the discovered instance to see us
	if err := p.ensureAndPublishSelfRecord(); err != nil {
		log.Printf("Warning: Failed to ensure/publish self record after discovery: %v", err)
	}

	// Publish all _pbSync records so all instances know about all other instances
	// This ensures manually added records are also synced
	if err := p.publishAllPBSyncRecords(); err != nil {
		log.Printf("Warning: Failed to publish all _pbSync records after discovery: %v", err)
	}

	log.Printf("Discovery complete for instance %s, all _pbSync records published", instanceID)
	return nil
}

// ensureAndPublishSelfRecord creates or updates the self record and publishes it
// This is called after discovery completes so the discovered instance can see us
func (p *plugin) ensureAndPublishSelfRecord() error {
	settings := p.app.Settings()
	host, port, err := nats.ParseNATSAddress(settings.Sync.NatsURL)
	if err != nil {
		return fmt.Errorf("failed to parse NATS address: %w", err)
	}

	natsAddress := fmt.Sprintf("%s:%d", host, port)

	// Check if record already exists
	collection, err := p.app.(*pocketbase.PocketBase).FindCollectionByNameOrId(syncpkg.PBSyncCollectionName)
	if err != nil {
		return fmt.Errorf("failed to find _pbSync collection: %w", err)
	}

	existing := &core.Record{}
	err = p.app.(*pocketbase.PocketBase).RecordQuery(collection.Id).
		AndWhere(dbx.HashExp{"instanceID": p.instanceID}).
		Limit(1).
		One(existing)

	var record *core.Record
	var isCreate bool

	if err == nil && existing.Id != "" {
		// Record exists, update it
		existing.Set("natsAddress", natsAddress)
		if err := p.app.(*pocketbase.PocketBase).Save(existing); err != nil {
			return fmt.Errorf("failed to update self record: %w", err)
		}
		record = existing
		isCreate = false
	} else {
		// Record doesn't exist, create it
		record, err = syncpkg.GetSelfRecord(p.app.(*pocketbase.PocketBase), p.instanceID, natsAddress)
		if err != nil {
			return fmt.Errorf("failed to create self record: %w", err)
		}
		isCreate = true
	}

	// Publish the self record so other instances can see us
	recordData := p.getAllRecordFields(record)
	var event *events.Event
	if isCreate {
		event = events.NewCreateEvent(
			p.instanceID,
			syncpkg.PBSyncCollectionName,
			record.Id,
			recordData,
		)
		log.Printf("Published self record (create): instanceID=%s, natsAddress=%s", p.instanceID, natsAddress)
	} else {
		event = events.NewUpdateEvent(
			p.instanceID,
			syncpkg.PBSyncCollectionName,
			record.Id,
			recordData,
		)
		log.Printf("Published self record (update): instanceID=%s, natsAddress=%s", p.instanceID, natsAddress)
	}
	p.publisher.PublishAsync(event)

	return nil
}

// publishAllPBSyncRecords publishes all _pbSync records to NATS JetStream
// This ensures all instances know about all other instances, including manually added ones
func (p *plugin) publishAllPBSyncRecords() error {
	collection, err := p.app.(*pocketbase.PocketBase).FindCollectionByNameOrId(syncpkg.PBSyncCollectionName)
	if err != nil {
		return fmt.Errorf("failed to find _pbSync collection: %w", err)
	}

	// Get all _pbSync records
	records := []*core.Record{}
	err = p.app.(*pocketbase.PocketBase).RecordQuery(collection.Id).All(&records)
	if err != nil {
		return fmt.Errorf("failed to fetch _pbSync records: %w", err)
	}

	// Publish each record (skip if we're currently syncing to avoid loops)
	p.mu.RLock()
	syncing := p.syncing
	p.mu.RUnlock()
	if syncing {
		// If we're syncing, don't publish (to avoid loops)
		return nil
	}

	for _, record := range records {
		recordData := p.getAllRecordFields(record)
		event := events.NewUpdateEvent(
			p.instanceID,
			syncpkg.PBSyncCollectionName,
			record.Id,
			recordData,
		)
		p.publisher.PublishAsync(event)
		log.Printf("Published _pbSync record: instanceID=%s", record.GetString("instanceID"))
	}

	log.Printf("Published %d _pbSync records to ensure all instances are aware of each other", len(records))
	return nil
}

func (p *plugin) startSnapshotJob() {
	interval := time.Duration(p.snapshotInterval) * time.Hour
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial snapshot after delay
	time.Sleep(5 * time.Second)
	if _, err := p.snapshotManager.CreateSnapshot(); err != nil {
		log.Printf("Warning: Failed to create initial snapshot: %v", err)
	}

	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			if _, err := p.snapshotManager.CreateSnapshot(); err != nil {
				log.Printf("Warning: Failed to create snapshot: %v", err)
			}
		}
	}
}

func (p *plugin) stopSync() {
	if p.stopChan != nil {
		close(p.stopChan)
	}
	if p.subscriber != nil {
		p.subscriber.Close()
	}
	if p.publisher != nil {
		p.publisher.Close()
	}
	if p.embeddedNATS != nil {
		p.embeddedNATS.Shutdown()
	}
	if p.realtimeManager != nil {
		p.realtimeManager.Close()
	}
}

func generateUUID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16])
}
