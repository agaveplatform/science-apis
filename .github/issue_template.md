## Issue summary 

Please provide a short summary of the issue or feature request. Extended descriptions and justification should be placed in the expected behavior section.

## What platform services are impacted?

* [ ] Apps
* [ ] Events
* [ ] Files
* [ ] Groups
* [ ] Jobs
* [ ] Logging
* [ ] Metadata
* [ ] Monitors
* [ ] Notifications
* [ ] Templates
* [ ] Postits
* [ ] Profiles
* [ ] Preferences
* [ ] Reactors
* [ ] Realtime
* [ ] Repositories
* [ ] Stats
* [ ] Systems
* [ ] Tags
* [ ] Tenants
* [ ] Transfers
* [ ] Transforms
* [ ] Usage
* [ ] Uuids

## To which tenant does this issue apply?

You can get this information from calling the `/tenants/v2` or invoking the `tenants-list` command in the CLI. 
Community - https://api.agaveplatform.org

## What version of the platform are you using?

You can get this information from the version field in every API response. The version in the response below is 2.2.6-r4f1bf09.

```
systems-list -V -l 0
Calling curl -sk -H "Authorization: Bearer $TOKEN" 'https://public.agaveapi.co/systems/v2/?pretty=true&limit=0'
{
  "status" : "success",
  "message" : null,
  "version" : "2.2.6-r4f1bf09",
  "result" : [ ]
}
```

## Steps to recreate the issue

1. 
2. 
3. 
4. 

## Expected behavior  

Provide a detailed description of the expeced behavior and how it differs from the current behavior.

## Actual behavior  

Provide a description of the actual behavior displayed in the version of the platform described above.
