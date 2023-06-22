package repotest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
)

func RunRepositoryTestSuite(t *testing.T, repo suppression.Repository) {
	token := []byte("token")

	t.Run("get the token before setting anything", func(t *testing.T) {
		rtoken, err := repo.GetToken()
		require.NoError(t, err)
		require.Nil(t, rtoken, "it should return nil when trying to get the token before setting it")
	})

	t.Run("adding suppressions", func(t *testing.T) {
		err := repo.Add([]model.Suppression{
			{
				WorkspaceID: "workspace1",
				UserID:      "user1",
				SourceIDs:   []string{},
				CreatedAt:   time.Date(2020, time.March, 27, 2, 2, 1, 2, time.UTC),
			},
			{
				WorkspaceID: "workspace2",
				UserID:      "user2",
				SourceIDs:   []string{"source1"},
				CreatedAt:   time.Date(2019, time.March, 27, 2, 2, 1, 2, time.UTC),
			},
		}, token)
		require.NoError(t, err, "it should be able to add some suppressions without an error")
	})

	t.Run("get token after setting it", func(t *testing.T) {
		rtoken, err := repo.GetToken()
		require.NoError(t, err)
		require.Equal(t, token, rtoken, "it should return the token that was previously set")
	})

	t.Run("wildcard suppression", func(t *testing.T) {
		suppressed, err := repo.Suppressed("workspace1", "user1", "source1")
		require.NoError(t, err)
		require.True(t, suppressed, "it should return true when trying to suppress a user that is suppressed by a wildcard suppression")
		createdAt, err := repo.GetCreatedAt("workspace1", "user1", "source1")
		require.NoError(t, err)
		require.Equal(t, time.Date(2020, time.March, 27, 2, 2, 1, 2, time.UTC), createdAt) // should be the same as the one we added

		suppressed, err = repo.Suppressed("workspace1", "user1", "source2")
		require.NoError(t, err)
		require.True(t, suppressed, "it should return true when trying to suppress a user that is suppressed by a wildcard suppression")
		createdAt, err = repo.GetCreatedAt("workspace1", "user1", "source2")
		require.NoError(t, err)
		require.Equal(t, time.Date(2020, time.March, 27, 2, 2, 1, 2, time.UTC), createdAt) // should be the same as the one we added
	})

	t.Run("exact suppression", func(t *testing.T) {
		suppressed, err := repo.Suppressed("workspace2", "user2", "source1")
		require.NoError(t, err)
		require.True(t, suppressed, "it should return true when trying to suppress a user that is suppressed by an exact suppression")
		createdAt, err := repo.GetCreatedAt("workspace2", "user2", "source1")
		require.NoError(t, err)
		require.Equal(t, time.Date(2019, time.March, 27, 2, 2, 1, 2, time.UTC), createdAt) // should be the same as the one we added
	})

	t.Run("non matching key", func(t *testing.T) {
		suppressed, err := repo.Suppressed("workspace3", "user3", "source2")
		require.NoError(t, err)
		require.False(t, suppressed, "it should return false when trying to suppress a user that is not suppressed")
		createdAt, err := repo.GetCreatedAt("workspace3", "user3", "source2")
		require.NoError(t, err)
		require.Equal(t, time.Time{}, createdAt) // should be the same as the one we added
	})

	t.Run("non matching suppression", func(t *testing.T) {
		suppressed, err := repo.Suppressed("workspace2", "user2", "source2")
		require.NoError(t, err)
		require.False(t, suppressed, "it should return false when trying to suppress a user that is suppressed for a different sourceID")
		createdAt, err := repo.GetCreatedAt("workspace2", "user2", "source2")
		require.NoError(t, err)
		require.Equal(t, time.Time{}, createdAt) // should be the same as the one we added
	})

	t.Run("canceling a suppression", func(t *testing.T) {
		suppressed, err := repo.Suppressed("workspace1", "user1", "source1")
		require.NoError(t, err)
		require.True(t, suppressed, "it should return true when trying to suppress a user that is suppressed by a wildcard suppression")

		token2 := []byte("token2")
		err = repo.Add([]model.Suppression{
			{
				WorkspaceID: "workspace1",
				Canceled:    true,
				UserID:      "user1",
				SourceIDs:   []string{},
			},
		}, token2)
		require.NoError(t, err)
		rtoken, err := repo.GetToken()
		require.NoError(t, err)
		require.Equal(t, token2, rtoken)

		suppressed, err = repo.Suppressed("workspace1", "user1", "source1")
		require.NoError(t, err)
		require.False(t, suppressed, "it should return false when trying to suppress a user that was suppressed by a wildcard suppression after the suppression has been canceled")
	})

	t.Run("multiple suppressions for the same userID", func(t *testing.T) {
		err := repo.Add([]model.Suppression{
			{
				WorkspaceID: "workspaceX",
				UserID:      "userX",
				SourceIDs:   []string{},
			},
			{
				WorkspaceID: "workspaceX",
				UserID:      "userX",
				SourceIDs:   []string{"source1"},
			},
			{
				WorkspaceID: "workspaceX",
				UserID:      "userX",
				SourceIDs:   []string{"source2"},
			},
		}, token)
		require.NoError(t, err, "it should be able to add some suppressions without an error")

		suppressed, err := repo.Suppressed("workspaceX", "userX", "sourceX")
		require.NoError(t, err)
		require.True(t, suppressed, "it should return true when trying to suppress a user that is suppressed by a wildcard suppression")

		require.NoError(t, repo.Add([]model.Suppression{
			{
				Canceled:    true,
				WorkspaceID: "workspaceX",
				UserID:      "userX",
				SourceIDs:   []string{},
			},
		}, token))
		suppressed, err = repo.Suppressed("workspaceX", "userX", "sourceX")
		require.NoError(t, err)
		require.False(t, suppressed, "it should return false when trying to suppress a user that is no longer suppressed by a wildcard suppression")

		suppressed, err = repo.Suppressed("workspaceX", "userX", "source1")
		require.NoError(t, err)
		require.True(t, suppressed, "it should return true when trying to suppress a user that is still suppressed by an exact match suppression")

		require.NoError(t, repo.Add([]model.Suppression{
			{
				Canceled:    true,
				WorkspaceID: "workspaceX",
				UserID:      "userX",
				SourceIDs:   []string{"source1"},
			},
		}, token))
		suppressed, err = repo.Suppressed("workspaceX", "userX", "source1")
		require.NoError(t, err)
		require.False(t, suppressed, "it should return false when trying to suppress a user that is no longer suppressed by an exact match suppression")

		suppressed, err = repo.Suppressed("workspaceX", "userX", "source2")
		require.NoError(t, err)
		require.True(t, suppressed, "it should return true when trying to suppress a user that is still suppressed by an exact match suppression")

		require.NoError(t, repo.Add([]model.Suppression{
			{
				Canceled:    true,
				WorkspaceID: "workspaceX",
				UserID:      "userX",
				SourceIDs:   []string{"source2"},
			},
		}, token))
		suppressed, err = repo.Suppressed("workspaceX", "userX", "source2")
		require.NoError(t, err)
		require.False(t, suppressed, "it should return false when trying to suppress a user that is no longer suppressed by an exact match suppression")
	})
}
