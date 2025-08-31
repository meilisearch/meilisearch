# Whole Sub-Object Filtering

Meilisearch supports filtering on specific sub-objects within array fields using a special syntax.

## The Problem

When filtering on fields within array objects, traditional filters (`field.subfield = value`) match across different sub-objects, which can lead to unexpected results.

Consider this example dataset:

```json
[
  { "id": 1, "users": [ { "name": "kero", "age": 28 }, { "name": "many", "age": 27 } ] },
  { "id": 2, "users": [ { "name": "kero", "age": 40 }, { "name": "tamo", "age": 28 } ] }
]
```

Using a traditional filter:

```
users.name = kero AND users.age = 28
```

This will match **both** documents because:
- Document 1 has a sub-object with `name = kero` and a different sub-object with `age = 28`
- Document 2 has a sub-object with `name = kero` and a different sub-object with `age = 28`

## The Solution: Sub-Object Filtering

To filter where all conditions must match on the **same** sub-object, use the sub-object filter syntax:

```
users { name = kero AND age = 28 }
```

This will match **only** document 1, because only it has a single sub-object where both `name = kero` AND `age = 28`.

## Syntax

The general syntax is:

```
array_field { condition1 AND condition2... }
```

Where:
- `array_field` is the name of the array field containing objects
- Inside the curly braces `{}` are the conditions to apply to each sub-object

## Examples

### Basic Example

```
users { name = kero AND age = 28 }
```

### Complex Conditions

You can use complex conditions within the sub-object filter:

```
users { (name = kero OR name = tamo) AND age >= 25 AND age <= 30 }
```

### Multiple Sub-Object Filters

You can combine multiple sub-object filters with AND/OR:

```
users { name = kero AND age = 28 } OR users { name = many AND age = 27 }
```

### Nested Objects

For nested objects, you can use dot notation within the sub-object filter:

```
users { name = kero AND contacts.email = "kero@example.com" }
```

## Limitations

- Sub-object filtering is only applicable to arrays of objects
- The maximum number of sub-objects in an array is limited to 256 per document
- Sub-object filtering may have a performance impact on large datasets

## Performance Considerations

Sub-object filtering requires additional indexing to track which values belong to which sub-objects. This may increase index size and slightly impact indexing performance for documents with large arrays of objects.
