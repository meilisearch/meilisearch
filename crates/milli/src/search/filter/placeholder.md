# Sub-Object Filtering Implementation Plan

## Feature Overview

Sub-object filtering allows users to filter on entire sub-objects within arrays, ensuring that all filter conditions apply to the same sub-object rather than potentially matching across different sub-objects.

## Example

Given documents like:
```json
[
  { "id": 1, "users": [ { "name": "kero", "age": 28 }, { "name": "many", "age": 27 } ] },
  { "id": 2, "users": [ { "name": "kero", "age": 40 }, { "name": "tamo", "age": 28 } ] }
]
```

Traditional filtering with `users.name = kero AND users.age = 28` would match both documents because the conditions can match across different sub-objects.

Sub-object filtering with `users { name = kero AND age = 28 }` would match only document 1 because only it has a sub-object where both conditions are true.

## Implementation Steps

1. Add a `SubObjectFilter` struct to represent the sub-object filter conditions
2. Update the filter parser to recognize the `field { conditions }` syntax
3. Implement the execution logic for sub-object filters
4. Add appropriate tests to verify the behavior

## Tracking

This feature is being tracked in issue #3642.
