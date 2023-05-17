package router

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	reportingPkg "github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/throttler"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	Reporting        reporter
	ErrorReporting   reporter
	Multitenant      tenantStats
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.MultiTenantJobsDB
	ProcErrorDB      jobsdb.JobsDB
	TransientSources transientsource.Service
	RsourcesService  rsources.JobService
	ThrottlerFactory *throttler.Factory
	Debugger         destinationdebugger.DestinationDebugger
	AdaptiveLimit    func(int64) int64
}

func (f *Factory) New(destination *backendconfig.DestinationT, identifier string) *HandleT {
	reportingEnabled := false
	config.RegisterBoolConfigVariable(types.DefaultReportingEnabled, &reportingEnabled, false, "Reporting.enabled")
	if f.ErrorReporting == nil || !reportingEnabled {
		f.ErrorReporting = &reportingPkg.NOOP{}
	}
	r := &HandleT{
		Reporting:        f.Reporting,
		ErrorReporting:   f.ErrorReporting,
		MultitenantI:     f.Multitenant,
		throttlerFactory: f.ThrottlerFactory,
		adaptiveLimit:    f.AdaptiveLimit,
	}
	destConfig := getRouterConfig(destination, identifier)
	r.Setup(
		f.BackendConfig,
		f.RouterDB,
		f.ProcErrorDB,
		destConfig,
		f.TransientSources,
		f.RsourcesService,
		f.Debugger,
	)
	return r
}

type destinationConfig struct {
	name          string
	responseRules map[string]interface{}
	config        map[string]interface{}
	destinationID string
}

func getRouterConfig(destination *backendconfig.DestinationT, identifier string) destinationConfig {
	return destinationConfig{
		name:          destination.DestinationDefinition.Name,
		destinationID: identifier,
		config:        destination.DestinationDefinition.Config,
		responseRules: destination.DestinationDefinition.ResponseRules,
	}
}
