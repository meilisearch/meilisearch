var request = null;
var timeoutID = null;
var display_facets = false;

$('#query, #filters').on('input', function () {
  var query = $('#query').val();
  var filters = $('#filters').val();
  var timeoutMs = 100;

  if (timeoutID !== null) {
    window.clearTimeout(timeoutID);
  }

  timeoutID = window.setTimeout(function () {
    request = $.ajax({
      type: "POST",
      url: "query",
      contentType: 'application/json',
      data: JSON.stringify({
        'query': query,
        'filters': filters,
        "facetDistribution": display_facets,
      }),
      contentType: 'application/json',
      success: function (data, textStatus, request) {
        results.innerHTML = '';
        facets.innerHTML = '';

        let timeSpent = request.getResponseHeader('Time-Ms');
        let numberOfDocuments = data.documents.length;
        count.innerHTML = data.numberOfCandidates.toLocaleString();
        time.innerHTML = `${timeSpent}ms`;
        time.classList.remove('fade-in-out');

        for (facet_name in data.facets) {
          for (value in data.facets[facet_name]) {
              const elem = document.createElement('span');
              const count = data.facets[facet_name][value];
              elem.classList.add("tag");
              elem.setAttribute('data-name', facet_name);
              elem.setAttribute('data-value', value);
              elem.innerHTML = `${facet_name}:${value} (${count})`;
              facets.appendChild(elem);
          }
        }

        for (element of data.documents) {
          const elem = document.createElement('li');
          elem.classList.add("document");

          const ol = document.createElement('ol');

          for (const prop in element) {
            const field = document.createElement('li');
            field.classList.add("field");

            const attribute = document.createElement('div');
            attribute.classList.add("attribute");
            attribute.innerHTML = prop;

            const content = document.createElement('div');
            content.classList.add("content");
            
            // Stringify Objects and Arrays to avoid [Object object]
            if (typeof element[prop] === 'object' && element[prop] !== null) {
              content.innerHTML = JSON.stringify(element[prop]);
            }  else {
              content.innerHTML = element[prop];
            }

            field.appendChild(attribute);
            field.appendChild(content);

            ol.appendChild(field);
          }

          elem.appendChild(ol);
          results.appendChild(elem);
        }

        // When we click on a tag we append the facet value
        // at the end of the facet query.
        $('#facets .tag').on('click', function () {
          let name = $(this).attr("data-name");
          let value = $(this).attr("data-value");

          let facet_query = $('#filters').val().trim();
          if (facet_query === "") {
            $('#filters').val(`${name} = "${value}"`).trigger('input');
          } else {
            $('#filters').val(`${facet_query} AND ${name} = "${value}"`).trigger('input');
          }
        });
      },
      beforeSend: function () {
        if (request !== null) {
          request.abort();
          time.classList.add('fade-in-out');
        }
      },
    });
  }, timeoutMs);
});

function diffArray(arr1, arr2) {
  return arr1.concat(arr2).filter(function (val) {
    if (!(arr1.includes(val) && arr2.includes(val)))
      return val;
  });
}

function selectedFacetsToArray(facets_obj) {
  var array = [];
  for (const facet_name in facets_obj) {
    var subarray = [];
    for (const facet_value of facets_obj[facet_name]) {
      subarray.push(`${facet_name}:${facet_value}`);
    }
    array.push(subarray);
  }
  return array;
}

$('#display_facets').click(function() {
  if (display_facets) {
    display_facets = false;
    $('#display_facets').html("Display facets")
    $('#display_facets').removeClass("is-danger");
    $('#display_facets').addClass("is-success");
    $('#facets').hide();
  } else {
    display_facets = true;
    $('#display_facets').html("Hide facets")
    $('#display_facets').addClass("is-danger");
    $('#display_facets').removeClass("is-success");
    $('#facets').show();
  }
});

// Make the number of document a little bit prettier
$('#docs-count').text(function(index, text) {
  return parseInt(text).toLocaleString()
});

// Make the database a little bit easier to read
$('#db-size').text(function(index, text) {
  return filesize(parseInt(text))
});

// We trigger the input when we load the script.
$(window).on('load', function () {
  // We execute a placeholder search when the input is empty.
  $('#query').trigger('input');
});
