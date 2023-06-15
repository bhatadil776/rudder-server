package marketobulkupload

import (
	"bufio"
	"bytes"
	"encoding/json"
	stdjson "encoding/json"
	"net/url"
	"os"
	"strings"
	time "time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

type MarketoBulkUploader struct {
	destName          string
	timeout           time.Duration
	destinationConfig map[string]interface{}
	TransformUrl      string
	PollUrl           string
	logger            logger.Logger
}

func NewBingAdsBulkUploader(destination *backendconfig.DestinationT, HTTPTimeout time.Duration) *MarketoBulkUploader {
	return &MarketoBulkUploader{
		destName:          "MARKETO_BULK_UPLOAD",
		timeout:           HTTPTimeout,
		destinationConfig: destination.DestinationDefinition.Config,
		TransformUrl:      "http://localhost:dummyport",
		PollUrl:           "/pollStatus",
		logger:            logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("BingAds").Child("MarketoBulkUploader"),
	}
}

func NewManager(destination *backendconfig.DestinationT, HTTPTimeout time.Duration) (*MarketoBulkUploader, error) {
	marketoBulkUpload := &MarketoBulkUploader{destName: "MARKETO_BULK_UPLOAD", timeout: HTTPTimeout, destinationConfig: destination.DestinationDefinition.Config, PollUrl: "/pollStatus", TransformUrl: config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")}
	return marketoBulkUpload, nil

}

type MarketoTransformerCall interface {
	HTTPCallWithRetryWithTimeout(url string, payload []byte, timeout time.Duration) ([]byte, int)
}

type MarketoTransformerImplCall struct{}

var (
	pkgLogger logger.Logger
)

func init() {
	pkgLogger = logger.NewLogger().Child("asyncdestinationmanager").Child("marketobulkupload")
}

func (m *MarketoTransformerImplCall) HTTPCallWithRetryWithTimeout(url string, payload []byte, timeout time.Duration) ([]byte, int) {
	var respBody []byte
	var statusCode int
	operation := func() error {
		var fetchError error
		respBody, statusCode, fetchError = misc.MakeHTTPRequestWithTimeout(url, bytes.NewBuffer(payload), timeout)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("Failed to make call. Error: %v, retrying after %v", err, t)
	})
	if err != nil {
		pkgLogger.Error("Error sending request to the server", err)
		return respBody, statusCode
	}
	return respBody, statusCode
}

func (b *MarketoBulkUploader) Poll(pollInput common.AsyncPoll) (common.PollStatusResponse, int) {
	payload, err := json.Marshal(pollInput)
	if err != nil {
		panic("JSON Marshal Failed" + err.Error())
	}
	m := MarketoTransformerImplCall{}
	bodyBytes, statusCode := m.HTTPCallWithRetryWithTimeout(b.TransformUrl+b.PollUrl, payload, b.timeout)
	var asyncResponse common.PollStatusResponse
	err = json.Unmarshal(bodyBytes, &asyncResponse)
	if err != nil {
		panic("JSON Unmarshal Failed" + err.Error())
	}
	return asyncResponse, statusCode
}

func GenerateFailedPayload(config map[string]interface{}, jobs []*jobsdb.JobT, importID, destType, csvHeaders string) []byte {
	var failedPayloadT common.AsyncFailedPayload
	failedPayloadT.Input = make([]map[string]interface{}, len(jobs))
	failedPayloadT.Config = config
	for index, job := range jobs {
		failedPayloadT.Input[index] = make(map[string]interface{})
		var message map[string]interface{}
		metadata := make(map[string]interface{})
		err := json.Unmarshal([]byte(common.GetTransformedData(job.EventPayload)), &message)
		if err != nil {
			panic("Unmarshalling Transformer Data to JSON Failed")
		}
		metadata["job_id"] = job.JobID
		failedPayloadT.Input[index]["message"] = message
		failedPayloadT.Input[index]["metadata"] = metadata
	}
	failedPayloadT.DestType = strings.ToLower(destType)
	failedPayloadT.ImportId = importID
	failedPayloadT.MetaData = common.MetaDataT{CSVHeaders: csvHeaders}
	payload, err := json.Marshal(failedPayloadT)
	if err != nil {
		panic("JSON Marshal Failed" + err.Error())
	}
	return payload
}

func (b *MarketoBulkUploader) GetUploadStats(UploadStatsInput common.FetchUploadJobStatus) (common.GetUploadStatsResponse, int) {
	transformUrl := config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	failedJobUrl := UploadStatsInput.FailedJobsURL
	parameters := UploadStatsInput.Parameters
	importId := gjson.GetBytes(parameters, "importId").String()
	csvHeaders := gjson.GetBytes(parameters, "metadata.csvHeader").String()
	payload := GenerateFailedPayload(b.destinationConfig, UploadStatsInput.ImportingList, importId, b.destName, csvHeaders)
	m := MarketoTransformerImplCall{}
	failedBodyBytes, statusCode := m.HTTPCallWithRetryWithTimeout(transformUrl+failedJobUrl, payload, b.timeout)
	if statusCode != 200 {
		return common.GetUploadStatsResponse{}, statusCode
	}
	var failedJobsResponse map[string]interface{}
	_ = json.Unmarshal(failedBodyBytes, &failedJobsResponse)

	internalStatusCode, _ := failedJobsResponse["status"].(string)
	metadata, ok := failedJobsResponse["metadata"].(map[string]interface{})
	if !ok {
		//TODO
		pkgLogger.Errorf("[Batch Router] Failed to typecast failed jobs response for Dest Type %v with statusCode %v and body %v", "MARKETO_BULK_UPLOAD", internalStatusCode, string(failedBodyBytes))
		return common.GetUploadStatsResponse{}, statusCode
	}

	failedKeys, errFailed := misc.ConvertStringInterfaceToIntArray(metadata["failedKeys"])
	warningKeys, errWarning := misc.ConvertStringInterfaceToIntArray(metadata["warningKeys"])
	succeededKeys, errSuccess := misc.ConvertStringInterfaceToIntArray(metadata["succeededKeys"])

	// Build the response body
	eventStatsResponse := common.GetUploadStatsResponse{
		Status: internalStatusCode,
		Metadata: common.EventStatMeta{
			FailedKeys:    failedKeys,
			ErrFailed:     errFailed,
			WarningKeys:   warningKeys,
			ErrWarning:    errWarning,
			SucceededKeys: succeededKeys,
			ErrSuccess:    errSuccess,
		},
	}
	return eventStatsResponse, statusCode
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

func (b *MarketoBulkUploader) Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	resolveURL := func(base, relative string) string {
		// var logger logger.Logger
		baseURL, _ := url.Parse(base)
		// if err != nil {
		// 	logger.Fatal(err)
		// }
		relURL, _ := url.Parse(relative)
		// if err != nil {
		// 	logger.Fatal(err)
		// }
		destURL := baseURL.ResolveReference(relURL).String()
		return destURL
	}
	destinationID := destination.ID
	destinationUploadUrl := asyncDestStruct.URL
	url := resolveURL(b.TransformUrl, destinationUploadUrl)
	filePath := asyncDestStruct.FileName
	config := destination.Config
	destType := destination.DestinationDefinition.Name
	failedJobIDs := asyncDestStruct.FailedJobIDs
	importingJobIDs := asyncDestStruct.ImportingJobIDs

	file, err := os.Open(filePath)
	if err != nil {
		panic("BRT: Read File Failed" + err.Error())
	}
	defer file.Close()
	var input []common.AsyncJob
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tempJob common.AsyncJob
		jobBytes := scanner.Bytes()
		err := json.Unmarshal(jobBytes, &tempJob)
		if err != nil {
			panic("Unmarshalling a Single Line Failed")
		}
		input = append(input, tempJob)
	}
	payload, err := json.Marshal(common.AsyncUploadT{
		Input:    input,
		Config:   config,
		DestType: strings.ToLower(destType),
	})
	if err != nil {
		panic("BRT: JSON Marshal Failed " + err.Error())
	}

	uploadTimeStat := stats.Default.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.TimerType, map[string]string{
		"module":   "batch_router",
		"destType": destType,
	})

	startTime := time.Now()
	payloadSizeStat.Observe(float64(len(payload)))
	pkgLogger.Debugf("[Async Destination Maanger] File Upload Started for Dest Type %v", destType)
	m := MarketoTransformerImplCall{}
	responseBody, statusCodeHTTP := m.HTTPCallWithRetryWithTimeout(url, payload, b.timeout)
	pkgLogger.Debugf("[Async Destination Maanger] File Upload Finished for Dest Type %v", destType)
	uploadTimeStat.Since(startTime)
	var bodyBytes []byte
	var statusCode string
	if statusCodeHTTP != 200 {
		bodyBytes = []byte(`"error" : "HTTP Call to Transformer Returned Non 200"`)
		return common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}
	}

	bodyBytes = responseBody
	statusCode = gjson.GetBytes(bodyBytes, "statusCode").String()

	var uploadResponse common.AsyncUploadOutput
	switch statusCode {
	case "200":
		var responseStruct common.UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		var parameters common.ImportParameters
		parameters.ImportId = responseStruct.ImportId
		url := responseStruct.PollUrl
		parameters.PollUrl = &url
		metaDataString, ok := responseStruct.Metadata["csvHeader"].(string)
		if !ok {
			parameters.MetaData = common.MetaDataT{CSVHeaders: ""}
		} else {
			parameters.MetaData = common.MetaDataT{CSVHeaders: metaDataString}
		}
		importParameters, err := json.Marshal(parameters)
		if err != nil {
			panic("Errored in Marshalling" + err.Error())
		}
		successfulJobIDs, failedJobIDsTrans := CleanUpData(responseStruct.Metadata, importingJobIDs)

		uploadResponse = common.AsyncUploadOutput{
			ImportingJobIDs:     successfulJobIDs,
			FailedJobIDs:        append(failedJobIDs, failedJobIDsTrans...),
			FailedReason:        `{"error":"Jobs flowed over the prescribed limit"}`,
			ImportingParameters: stdjson.RawMessage(importParameters),
			ImportingCount:      len(importingJobIDs),
			FailedCount:         len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID:       destinationID,
		}
	case "400":
		var responseStruct common.UploadStruct
		err := json.Unmarshal(bodyBytes, &responseStruct)
		if err != nil {
			panic("Incorrect Response from Transformer: " + err.Error())
		}
		eventsAbortedStat := stats.Default.NewTaggedStat("events_delivery_aborted", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": destType,
		})
		abortedJobIDs, failedJobIDsTrans := CleanUpData(responseStruct.Metadata, importingJobIDs)
		eventsAbortedStat.Count(len(abortedJobIDs))
		uploadResponse = common.AsyncUploadOutput{
			AbortJobIDs:   abortedJobIDs,
			FailedJobIDs:  append(failedJobIDs, failedJobIDsTrans...),
			FailedReason:  `{"error":"Jobs flowed over the prescribed limit"}`,
			AbortReason:   string(bodyBytes),
			AbortCount:    len(importingJobIDs),
			FailedCount:   len(failedJobIDs) + len(failedJobIDsTrans),
			DestinationID: destinationID,
		}
	default:
		uploadResponse = common.AsyncUploadOutput{
			FailedJobIDs:  append(failedJobIDs, importingJobIDs...),
			FailedReason:  string(bodyBytes),
			FailedCount:   len(failedJobIDs) + len(importingJobIDs),
			DestinationID: destinationID,
		}

	}
	return uploadResponse

}
