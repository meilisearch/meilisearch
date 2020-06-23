## v0.13.0 (unreleased)

  - placeholder search (#771)
  - Add database version mismatch check (#794)

## v0.12.0

  - Fix long documents not being indexed completely bug (#816)
  - Fix distinct attribute returning id instead of name (#800)
  - error code rename (#805)

## v0.11.1

  - Fix facet cache on document update (#789) 
  - Improvements on settings consistency (#778)

## v0.11.0

  - Change the HTTP framework, moving from tide to actix-web (#601)
  - Bump sentry version to 0.18.1 (#690)
  - Enable max payload size override (#684)
  - Disable sentry in debug (#681)
  - Better terminal greeting (#680)
  - Fix highlight misalignment (#679)
  - Add support for facet count (#676)
  - Add support for faceted search (#631)
  - Add support for configuring the lmdb map size (#646, #647)
  - Add exposed port for Dockerfile (#654)
  - Add sentry probe (#664)
  - Fix url trailing slash and double slash issues (#659)
  - Fix accept all Content-Type by default (#653)
  - Return the error message from Serde when a deserialization error is encountered (#661)
  - Fix NormalizePath middleware to make the dashboard accessible (#695)
  - Update sentry features to remove openssl (#702)
  - Add SSL support (#669)
  - Rename fieldsFrequency into fieldsDistribution in stats (#719)
  - Add support for error code reporting (#703)
  - Allow the dashboard to query private servers (#732)
  - Add telemetry (#720)
  - Add post route for search (#735)

## v0.10.1

  - Add support for floating points in filters (#640)
  - Add '@' character as tokenizer separator (#607)
  - Add support for filtering on arrays of strings (#611)

## v0.10.0

  - Refined filtering (#592)
  - Add the number of hits in search result (#541)
  - Add support for aligned crop in search result (#543)
  - Sanitize the content displayed in the web interface (#539)
  - Add support of nested null, boolean and seq values (#571 and #568, #574)
  - Fixed the core benchmark (#576)
  - Publish an ARMv7 and ARMv8 binaries on releases (#540 and #581)
  - Fixed a bug where the result of the update status after the first update was empty (#542)
  - Fixed a bug where stop words were not handled correctly (#594)
  - Fix CORS issues (#602)
  - Support wildcard on attributes to retrieve, highlight, and crop (#549, #565, and #598)
