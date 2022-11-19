#### Get information about different tables in the dataset
```
SELECT * from `<data-set-id>.__TABLES__` 
```

#### Sample events form a table
To explore the data in a table, use sampling instead of a normal query as it is cost efficient
```
SELECT * FROM `<table-id>` TABLESAMPLE SYSTEM (.001 percent)
```

#### Get issues
```
SELECT json_payload.issue.id as issue_id, json_payload.issue.number as issue_no, issue_url, payload, repo, org, created_at, id, other FROM 
    (SELECT
        id, SAFE.PARSE_JSON(payload) AS json_payload, JSON_VALUE(payload, '$.action') AS action, JSON_QUERY(payload, '$.issue.url') as issue_url, payload, repo, org, created_at, other
    FROM `githubarchive.month.20*`
    WHERE _TABLE_SUFFIX BETWEEN '1412' and '2300'
    AND type = 'IssuesEvent'
    ) WHERE action = 'opened' AND issue_url IS NOT NULL

```

#### Get issue comments
```
SELECT issue_id, issue_no, comment_id, comment_url, payload, repo, org, created_at, id, other FROM 
    (SELECT
        id, JSON_VALUE(payload, '$.issue.id') AS issue_id, JSON_VALUE(payload, '$.issue.number') as issue_no, JSON_VALUE(payload, '$.comment.id') as comment_id, JSON_VALUE(payload, '$.comment.url') as comment_url, JSON_QUERY(payload, '$.issue.pull_request') as pull_request,payload, repo, org, created_at, other
    FROM `githubarchive.month.20*`
    WHERE _TABLE_SUFFIX BETWEEN '1412' and '2300'
    AND type = 'IssueCommentEvent'
    ) WHERE comment_url IS NOT NULL AND pull_request IS NULL

```

#### Get pre-2015 issues
```
SELECT tb1.json_payload.issue as issue_id, tb1.json_payload.number as issue_no, payload, repo, org, created_at, id, other FROM
( 
  select SAFE.PARSE_JSON(payload) as json_payload, JSON_QUERY(payload, '$.action') as action, JSON_QUERY(payload, '$.issue.url') as issue_url,  *
  from `githubarchive.year.201*`
  WHERE type = 'IssuesEvent'
  AND _TABLE_SUFFIX BETWEEN '1' and '5'
) tb1
WHERE tb1.action = '"opened"' AND tb1.issue_url IS NULL
```

#### Get pre-2015 issue comments
```
SELECT issue_id, comment_id, comment_url, payload, repo, org, created_at, id, other FROM
( 
  select JSON_VALUE(payload, '$.comment_id') as comment_id, JSON_VALUE(payload, '$.issue_id') as issue_id, JSON_VALUE(other, '$.url') as comment_url, payload, repo, org, created_at, id, other
  from `githubarchive.month.201*`
  WHERE _TABLE_SUFFIX BETWEEN '000' AND '501' AND type = 'IssueCommentEvent'
) tb1
WHERE comment_id IS NOT NULL AND NOT CONTAINS_SUBSTR(tb1.comment_url, '/pull/')
LIMIT 100
```

#### Issues filtered
```
select stars, html_url as repo_url, issue_id, issue_no, title, body FROM 
`<dataset-id>.100-star-repos` as t1 INNER JOIN
(select issue_id, issue_no, issue_url, repo, JSON_VALUE(payload, '$.issue.title') as title, JSON_VALUE(payload, '$.issue.body') as body from `<dataset-id>.issues`) as t2 ON SUBSTR(t1.html_url, 20) = t2.repo.name
```

#### Issue comments filtered
```
select stars, html_url as repo_url, issue_id, issue_no, comment_id, title, body, comment, created_at FROM 
`<dataset-id>.100-star-repos` as t1 INNER JOIN
(select issue_id, issue_no, comment_id, repo, JSON_VALUE(payload, '$.issue.title') as title, JSON_VALUE(payload, '$.issue.body') as body, JSON_VALUE(payload, '$.comment.body') as comment, created_at from 
`<dataset-id>.issue-comments`) as t2 
ON SUBSTR(t1.html_url, 20) = t2.repo.name
```

#### Star count per repo
```
SELECT 
  COUNT(*) naive_count,
  COUNT(DISTINCT actor.id) unique_by_actor_id, 
  COUNT(DISTINCT actor.login) unique_by_actor_login, repo.id, repo.url
FROM `githubarchive.day.2*`
where type = 'WatchEvent'
GROUP BY repo.id, repo.url
```
There is additional post processing done on top of the results of this to unify the repo url to same format since different events have different format of the url (https://github.com, https://api.github.com etc)

#### pre-2015 issues filtered by 100-star repos
```
select t1.stars as repo_stars, t1.html_url as repo_url, t2.issue_id, t2.issue_no 
    FROM `<dataset-id>.100-star-repos` as t1 
    INNER JOIN `<dataset-id>.pre-2015-issues` as t2 
    ON t1.html_url = t2.repo.url
```

#### pre-2015 issue comments filtered by 100-star repo list
```
select t1.html_url as repo_url, t1.stars as repo_stars, t2.issue_id, t2.comment_id, t2.comment_url 
    FROM `<dataset-id>.100-star-repos` as t1 
    INNER JOIN `<dataset-id>.pre-2015-issue-comments` as t2 
    ON t1.html_url = t2.repo.url
```

