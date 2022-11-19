
Primary source of data is the bigquery [githubarchive](https://www.gharchive.org/) dataset. Raw dumps can also be downloaded from https://www.gharchive.org/ directly. But since the data is large (17TB in bigquery), downloading and analyzing them will be an effort. So, going with bigquery is a reasonable choice to begin with.

Information in githubarchive is events data, it's not a snapshot of github data.There will be multiple events (create, update, delete etc) for same github resource (issue, repo etc).

bigquery data has a top level field called "type" which is the event type, based on which we can filter the events that are of interest to us.

Events that are of interest to us are IssueCommentEvent, IssuesEvent. Read more about these events [here](https://docs.github.com/en/developers/webhooks-and-events/events/github-event-types). This documentation says that the `payload.action` field can be "created", "edited" or "deleted". but, bigquery seems to only contain data for "created" action. It is clarified [here](https://github.com/igrigorik/gharchive.org/issues/183) on the fact that edit events are not part of the gharchive.

Github APIs treat both issues and pull request in a similar manner. [Ref](https://docs.github.com/en/rest/issues/issues). `IssuesEvent` contains events related to issue + pull request creation/closed events. `IssueCommentEvent` contains events related to issue + pull request comments. So, we need to exclude events related to pull requests.

Data format for pre-2015 and later periods is different. pre-2015 data contains only the issue id and comment ids while the later data contains title and body as well. So, we need to get the content for pre-2015 data by some other means.

`WatchEvent` can be used to get the list of repos by number of stars. Below query gets the list of repos and the number of stars
```
SELECT 
  COUNT(*) naive_count,
  COUNT(DISTINCT actor.id) unique_by_actor_id, 
  COUNT(DISTINCT actor.login) unique_by_actor_login, repo.id, repo.url
FROM `githubarchive.day.2*`
where type = 'WatchEvent'
GROUP BY repo.id, repo.url
```
Note that the number of stars from this query is only approximate. Read [SO Post](https://stackoverflow.com/questions/42918135/how-to-get-total-number-of-github-stars-for-a-given-repo-in-bigquery) to understand the nuances around the star counts.

Data in big query is organized in three ways (day/month/year wise). Use the daily tables for exploration and testing since bigquery pricing is per the data that gets scanned during query execution.

Issue, comments data is extracted from monthly tables on 27,28th Oct 2022.
Repo list is extracted from daily tables on 30th Oct 2022.

#### Some stats
Total repos extracted = ~25.7M
Repos with <= 100 stars = ~324K


post 2015 issue = ~85M issues
pre 2015 issues = ~9.4M issues
post 2015 issue comments = ~156M
pre 2015 issue comments = ~17.7M


filtered post 2015 issues = ~29.5M
filtered pre 2015 issues = ~2.9M
filtered post 2015 issue comments = ~100M
filtered pre 2015 issue comments = ~11M


#### Other data sources explored:
[ghtorrent](https://ghtorrent.org/) project doesn't seem to be active. Data is there only till 2019. Even that, we can only get the issue ids.
Bigquery public dataset `github_repo` doesn't have data related to issues and comments. It only has code.



