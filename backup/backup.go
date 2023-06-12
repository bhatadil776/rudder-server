package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
)

type jobQueue interface {
	GetProcessed(
		ctx context.Context,
		params jobsdb.GetQueryParamsT,
	) (jobsdb.JobsResult, error)

	UpdateJobStatus(
		ctx context.Context,
		statusList []*jobsdb.JobStatusT,
		customValFilters []string,
		parameterFilters []jobsdb.ParameterFilterT,
	) error
}

type BackupContext struct {
	QueryParams          jobsdb.GetQueryParamsT
	Queue                jobQueue
	FileUploaderProvider fileuploader.Provider
}

func Backup(
	ctx context.Context,
	backupContext BackupContext,
	log logger.Logger,
) {
	// retry and backoff applied to all the steps below

	// 1. Get the jobs from the jobsdb
	jobs, err := backupContext.Queue.GetProcessed(ctx, backupContext.QueryParams)
	if err != nil {
		log.Infof("backup Error: Error getting jobs from jobsdb: %w", err)
		panic(err)
	}
	workspaceJobsMap := lo.GroupBy(jobs.Jobs, func(job *jobsdb.JobT) string {
		return job.WorkspaceId
	})
	// 2. Upload the jobs to the file uploader
	for workspaceID, wJobs := range workspaceJobsMap {
		// write to file
		backupPathDirName := "/rudder-s3-dumps/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			panic(err)
		}
		// pathPrefix := strings.TrimPrefix("gw_jobs", preDropTablePrefix)
		pathPrefix := "gw_jobs" //TODO: remove
		path := fmt.Sprintf(
			"%v%v.%v.%v.%v.%v.%v.gz",
			tmpDirPath+backupPathDirName,
			pathPrefix,
			wJobs[0].JobID,
			wJobs[len(wJobs)-1].JobID,
			wJobs[0].CreatedAt.UnixNano()/int64(time.Millisecond),
			wJobs[len(wJobs)-1].CreatedAt.UnixNano()/int64(time.Millisecond),
			workspaceID,
		)

		err = WriteJobsToFile(wJobs, path)
		if err != nil {
			panic(err)
		}

		// upload file
		pathPrefixes := make([]string, 0) // TOOO
		var output filemanager.UploadOutput
		fileUploader, err := backupContext.FileUploaderProvider.GetFileManager(workspaceID)
		if err != nil {
			panic(err)
		}
		bo := backoff.NewExponentialBackOff()
		bo.MaxInterval = time.Minute
		bo.MaxElapsedTime = config.GetDuration("backup.maxRetryTime", 5, time.Minute)
		boRetries := backoff.WithMaxRetries(bo, uint64(config.GetInt64("backup.maxRetries", 3)))
		boCtx := backoff.WithContext(boRetries, ctx)
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer func() { _ = file.Close() }()
		backup := func() error {
			output, err = fileUploader.Upload(ctx, file, pathPrefixes...)
			return err
		}
		if err = backoff.Retry(backup, boCtx); err != nil {
			panic(err)
		}
		log.Infof("[JobsDB] :: Backed up table at %s for workspaceId %s", output.Location, workspaceID)
		// 3. Update the job status in the jobsdb
		err = backupContext.Queue.UpdateJobStatus(
			ctx,
			lo.Map(
				wJobs,
				func(job *jobsdb.JobT, _ int) *jobsdb.JobStatusT {
					js := &jobsdb.JobStatusT{
						JobID:         job.JobID,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "",
						ErrorResponse: []byte(`{}`), // check
						Parameters:    []byte(`{}`), // check
						JobParameters: job.Parameters,
					}
					if job.LastJobStatus.ErrorCode == "200" {
						js.JobState = jobsdb.Succeeded.State
					}
					js.JobState = jobsdb.Aborted.State
					return js
				},
			),
			nil,
			nil,
		)
	}
}

// Writes a list of jobs to a .gz file at given path
func WriteJobsToFile(jobs []*jobsdb.JobT, path string) error {
	gzipFilePath := fmt.Sprintf(`%v.gz`, path)
	err := os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	gzWriter, err := misc.CreateGZ(gzipFilePath)
	if err != nil {
		panic(err)
	}
	defer gzWriter.CloseGZ()

	// gzWriter.Write([]byte(jobs[0].Headings()))
	for _, item := range jobs {
		err = gzWriter.WriteGZ(item.ToCSV())
		if err != nil {
			return err
		}
	}

	return nil
}
