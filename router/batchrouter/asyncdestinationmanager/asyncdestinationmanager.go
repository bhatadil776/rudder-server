package asyncdestinationmanager

import (
	stdjson "encoding/json"
	"fmt"
	"strings"
	"sync"
	time "time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	marketobulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

type Asyncdestinationmanager interface {
	Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput
	Poll(url string, payload []byte, timeout time.Duration) ([]byte, int)
}

type AsyncDestinationStruct struct {
	ImportingJobIDs []int64
	FailedJobIDs    []int64
	Exists          bool
	Size            int
	CreatedAt       time.Time
	FileName        string
	Count           int
	CanUpload       bool
	UploadMutex     sync.RWMutex
	URL             string
	RsourcesStats   rsources.StatsCollector
}

type AsyncUploadT struct {
	Config   map[string]interface{} `json:"config"`
	Input    []AsyncJob             `json:"input"`
	DestType string                 `json:"destType"`
}

type AsyncJob struct {
	Message  map[string]interface{} `json:"message"`
	Metadata map[string]interface{} `json:"metadata"`
}

type AsyncFailedPayload struct {
	Config   map[string]interface{}   `json:"config"`
	Input    []map[string]interface{} `json:"input"`
	DestType string                   `json:"destType"`
	ImportId string                   `json:"importId"`
	MetaData MetaDataT                `json:"metadata"`
}

type MetaDataT struct {
	CSVHeaders string `json:"csvHeader"`
}

type UploadStruct struct {
	ImportId string                 `json:"importId"`
	PollUrl  string                 `json:"pollURL"`
	Metadata map[string]interface{} `json:"metadata"`
}

type Parameters struct {
	ImportId string    `json:"importId"`
	PollUrl  string    `json:"pollURL"`
	MetaData MetaDataT `json:"metadata"`
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var (
	HTTPTimeout time.Duration
	pkgLogger   logger.Logger
)

func loadConfig() {
	config.RegisterDurationConfigVariable(600, &HTTPTimeout, true, time.Second, "AsyncDestination.HTTPTimeout")
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("asyncDestinationManager")
}

func CleanUpData(keyMap map[string]interface{}, importingJobIDs []int64) ([]int64, []int64) {
	if keyMap == nil {
		return []int64{}, importingJobIDs
	}

	_, ok := keyMap["successfulJobs"].([]interface{})
	var succesfulJobIDs, failedJobIDsTrans []int64
	var err error
	if ok {
		succesfulJobIDs, err = misc.ConvertStringInterfaceToIntArray(keyMap["successfulJobs"])
		if err != nil {
			failedJobIDsTrans = importingJobIDs
		}
	}
	_, ok = keyMap["unsuccessfulJobs"].([]interface{})
	if ok {
		failedJobIDsTrans, err = misc.ConvertStringInterfaceToIntArray(keyMap["unsuccessfulJobs"])
		if err != nil {
			failedJobIDsTrans = importingJobIDs
		}
	}
	return succesfulJobIDs, failedJobIDsTrans
}

func GetTransformedData(payload stdjson.RawMessage) string {
	return gjson.Get(string(payload), "body.JSON").String()
}

func GetMarshalledData(payload string, jobID int64) string {
	var job AsyncJob
	err := json.Unmarshal([]byte(payload), &job.Message)
	if err != nil {
		panic("Unmarshalling Transformer Response Failed")
	}
	job.Metadata = make(map[string]interface{})
	job.Metadata["job_id"] = jobID
	responsePayload, err := json.Marshal(job)
	if err != nil {
		panic("Marshalling Response Payload Failed")
	}
	return string(responsePayload)
}

func GenerateFailedPayload(config map[string]interface{}, jobs []*jobsdb.JobT, importID, destType, csvHeaders string) []byte {
	var failedPayloadT AsyncFailedPayload
	failedPayloadT.Input = make([]map[string]interface{}, len(jobs))
	index := 0
	failedPayloadT.Config = config
	for _, job := range jobs {
		failedPayloadT.Input[index] = make(map[string]interface{})
		var message map[string]interface{}
		metadata := make(map[string]interface{})
		err := json.Unmarshal([]byte(GetTransformedData(job.EventPayload)), &message)
		if err != nil {
			panic("Unmarshalling Transformer Data to JSON Failed")
		}
		metadata["job_id"] = job.JobID
		failedPayloadT.Input[index]["message"] = message
		failedPayloadT.Input[index]["metadata"] = metadata
		index++
	}
	failedPayloadT.DestType = strings.ToLower(destType)
	failedPayloadT.ImportId = importID
	failedPayloadT.MetaData = MetaDataT{CSVHeaders: csvHeaders}
	payload, err := json.Marshal(failedPayloadT)
	if err != nil {
		panic("JSON Marshal Failed" + err.Error())
	}
	return payload
}

func NewManager(destination *backendconfig.DestinationT) Asyncdestinationmanager {
	destType := destination.DestinationDefinition.Name
	if destType == "BING_ADS" {
		return marketobulkupload.NewManager()
	} else if destType == "MARKETO_BULK_UPLOAD" {
		return marketobulkupload.NewManager()
	} else {
		panic(fmt.Errorf("batch router is not enabled for destination %s", destType))
	}
}
