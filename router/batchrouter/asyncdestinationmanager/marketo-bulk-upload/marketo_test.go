package marketobulkupload

import (
	"encoding/json"
	"sync"
	"testing"
	time "time"

	"github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_bulkservice "github.com/rudderlabs/rudder-server/mocks/router/marketo_bulk_upload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/stretchr/testify/assert"
)

func TestMarketoUpload(t *testing.T) {
	ctrl := gomock.NewController(t)
	marketoBulkService := mock_bulkservice.NewMockMarketoTransformerCall(ctrl)

	data := map[string]interface{}{
		"ID": "1mMy5cqbtfuaKZv1IhVQKnBdVwe",
		"Config": map[string]interface{}{
			"munchkinId":   "XXXX",
			"clientId":     "YYYY",
			"clientSecret": "ZZZZ",
			"columnFieldsMapping": []map[string]interface{}{
				{
					"to":   "name__c",
					"from": "name",
				},
				{
					"to":   "email__c",
					"from": "email",
				},
				{
					"to":   "plan__c",
					"from": "plan",
				},
			},
		},
	}

	// Populate the structs
	destinationDefinition := backendconfig.DestinationDefinitionT{
		ID:            "destination_definition_id",
		Name:          "destination_definition_name",
		DisplayName:   "destination_definition_display_name",
		Config:        data["Config"].(map[string]interface{}),
		ResponseRules: map[string]interface{}{
			// Populate ResponseRules if available
		},
	}

	destination := backendconfig.DestinationT{
		ID:                    "destination_id",
		Name:                  "destination_name",
		DestinationDefinition: destinationDefinition,
		Config:                nil, // Assign the Config field separately
		Enabled:               true,
		WorkspaceID:           "workspace_id",
		Transformations:       nil, // Assign the Transformations field separately
		IsProcessorEnabled:    true,
		RevisionID:            "revision_id",
	}

	// Assign Config field separately
	destination.Config = data["Config"].(map[string]interface{})
	asyncStructData := common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{4, 5},
		Exists:          true,
		Size:            1024,
		CreatedAt:       time.Now(),
		FileName:        "example.txt",
		Count:           100,
		CanUpload:       false,
		UploadMutex:     sync.RWMutex{},
		URL:             "https://example.com",
		RsourcesStats:   nil,
	}

	bulkUploader := NewBingAdsBulkUploader(&destination, 10*time.Second)

	uploadData := common.UploadStruct{
		ImportId: "2977",
		PollUrl:  "/pollStatus",
		Metadata: map[string]interface{}{
			"successfulJobs":   []string{"17"},
			"unsuccessfulJobs": []string{},
			"csvHeader":        "email",
		},
	}

	uploadDataInBytes, _ := json.Marshal(uploadData)

	marketoBulkService.EXPECT().HTTPCallWithRetryWithTimeout(gomock.Any(), gomock.Any(), gomock.Any()).Return(uploadDataInBytes, 200).Times(1)

	expectedResp := common.AsyncUploadOutput{
		ImportingJobIDs:     []int64{1, 2, 3},
		ImportingParameters: json.RawMessage(`{"param1": "value1", "param2": "value2"}`),
		FailedJobIDs:        []int64{4, 5},
		FailedReason:        "Failed to import data",
		ImportingCount:      3,
		FailedCount:         2,
		DestinationID:       "destination_id",
	}
	recievedResponse := bulkUploader.Upload(&destination, &asyncStructData)

	assert.Equal(t, recievedResponse, expectedResp)
}
