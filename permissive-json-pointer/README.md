# Permissive json pointer

This crate provide an interface a little bit similar to what you know as “json pointer”.
But it’s actually doing something quite different.

## The API

The crate provide only one function called [`select_values`].
It takes one object in parameter and a list of selectors.
It then returns a new object containing only the fields you selected.

## The selectors

The syntax for the selector is easier than with other API.
There is only ONE special symbol, it’s the `.`.

If you write `dog` and provide the following object;
```json
{
  "dog": "bob",
  "cat": "michel"
}
```
You’ll get back;
```json
{
  "dog": "bob",
}
```

Easy right?

Now the dot can either be used as a field name, or as a nested object.

For example, if you have the following json;
```json
{
  "dog.name": "jean",
  "dog": {
    "name": "bob",
    "age": 6
  }
}
```

What a crappy json! But never underestimate your users, they [_WILL_](https://xkcd.com/1172/)
somehow base their entire workflow on this kind of json.
Here with the `dog.name` selector both fields will be
selected and the following json will be returned;
```json
{
  "dog.name": "jean",
  "dog": {
    "name": "bob",
  }
}
```

And as you can guess, this crate is as permissive as possible.
It’ll match everything it can!
Consider this even more crappy json;
```json
{
  "pet.dog.name": "jean",
  "pet.dog": {
    "name": "bob"
  },
  "pet": {
    "dog.name": "michel"
  },
  "pet": {
    "dog": {
      "name": "milan"
    }
  }
}
```
If you write `pet.dog.name` everything will be selected.

## Matching arrays

With this kind of selectors you can’t match a specific element in an array.
Your selector will be applied to all the element _in_ the array.

Consider the following json;
```json
{
  "pets": [
    {
      "animal": "dog",
      "race": "bernese mountain",
    },
    {
      "animal": "dog",
      "race": "golden retriever",
    },
    {
      "animal": "cat",
      "age": 8,
    }
  ]
}
```

With the filter `pets.animal` you’ll get;
```json
{
  "pets": [
    {
      "animal": "dog",
    },
    {
      "animal": "dog",
    },
    {
      "animal": "cat",
    }
  ]
}
```

The empty element in an array gets removed. So if you were to look
for `pets.age` you would only get;
```json
{
  "pets": [
    {
      "age": 8,
    }
  ]
}
```

And I think that’s all you need to know 🎉