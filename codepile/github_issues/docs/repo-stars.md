Get number of stars by repo
```
SELECT 
  COUNT(*) naive_count,
  COUNT(DISTINCT actor.id) unique_by_actor_id, 
  COUNT(DISTINCT actor.login) unique_by_actor_login, repo.id, repo.url
FROM `githubarchive.day.2*`
where type = 'WatchEvent'
GROUP BY repo.url
```

Some of the events don't contain repo.id and some don't have actor.id. So, `unique_by_actor_login` is the most accurate of all the counts.

repo.url takes values of different format over the time period. Some have urls of the format https://api.github.com/repos/... while some have https://api.github.dev/repos/...

Result of big query is further processed to get the repo url into single format and then sum stars by new url. This list is further filtered down to get the repos that have >= 100 stars.