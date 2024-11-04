# Flatten serde Json

This crate flatten [`serde_json`](https://docs.rs/serde_json/latest/serde_json/) `Object` in a format
similar to [elastic search](https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html).

## Examples

### There is nothing to do

```json
{
  "id": "287947",
  "title": "Shazam!",
  "release_date": 1553299200,
  "genres": [
    "Action",
    "Comedy",
    "Fantasy"
  ]
}
```

Flattens to:
```json
{
  "id": "287947",
  "title": "Shazam!",
  "release_date": 1553299200,
  "genres": [
    "Action",
    "Comedy",
    "Fantasy"
  ]
}
```

------------

### Objects

```json
{
  "a": {
    "b": "c",
    "d": "e",
    "f": "g"
  }
}
```

Flattens to:
```json
{
  "a.b": "c",
  "a.d": "e",
  "a.f": "g"
}
```

------------

### Array of objects

```json
{
  "a": [
    { "b": "c" },
    { "b": "d" },
    { "b": "e" },
  ]
}
```

Flattens to:
```json
{
  "a.b": ["c", "d", "e"],
}
```

------------

### Array of objects with normal value in the array

```json
{
  "a": [
    42,
    { "b": "c" },
    { "b": "d" },
    { "b": "e" },
  ]
}
```

Flattens to:
```json
{
  "a": 42,
  "a.b": ["c", "d", "e"],
}
```

------------

### Array of objects of array of objects of ...

```json
{
  "a": [
    "b",
    ["c", "d"],
    { "e": ["f", "g"] },
    [
        { "h": "i" },
        { "e": ["j", { "z": "y" }] },
    ],
    ["l"],
    "m",
  ]
}
```

Flattens to:
```json
{
  "a": ["b", "c", "d", "l", "m"],
  "a.e": ["f", "g", "j"],
  "a.h": "i",
  "a.e.z": "y",
}
```

------------

### Collision between a generated field name and an already existing field

```json
{
  "a": {
    "b": "c",
  },
  "a.b": "d",
}
```

Flattens to:
```json
{
  "a.b": ["c", "d"],
}
```

