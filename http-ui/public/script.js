var request = null;
var timeoutID = null;
var selected_facets = {};

$('#query, #filters').on('input', function () {
  var query = $('#query').val();
  var filters = $('#filters').val();
  var facet_filters = selectedFacetsToArray(selected_facets);
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
        'facetFilters': facet_filters,
        "facetDistribution": true,
      }),
      contentType: 'application/json',
      success: function (data, textStatus, request) {
        documents.innerHTML = '';
        facets.innerHTML = '';

        let timeSpent = request.getResponseHeader('Time-Ms');
        let numberOfDocuments = data.documents.length;
        count.innerHTML = data.numberOfCandidates.toLocaleString();
        time.innerHTML = `${timeSpent}ms`;
        time.classList.remove('fade-in-out');

        for (facet_name in data.facets) {
          // Append an header to the list of facets
          let upperCaseName = facet_name.charAt(0).toUpperCase() + facet_name.slice(1);
          $("<h3></h3>").text(upperCaseName).appendTo($('#facets'));

          // Create a div for a bulma select
          const header = document.createElement('div');
          let div = $("<div></div>").addClass('select is-multiple');

          // Create the select element
          let select = $(`<select data-facet-name='${facet_name}' multiple size=\"8\"></select>`);
          let selected_values = selected_facets[facet_name] || [];
          // Create the previously selected facets (mark them as selected)
          for (value of selected_values) {
              let option = $('<option></option>')
                .text(value)
                .attr('selected', "selected")
                .attr('value', value)
                .attr('title', value);
              select.append(option);
          }

          // Create the newly discovered facets
          let diff = diffArray(data.facets[facet_name], selected_values);
          for (value of diff) {
              let option = $('<option></option>')
                .text(value)
                .attr('value', value)
                .attr('title', value);
              select.append(option);
          }

          div.append(select);
          $('#facets').append(div);
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
            content.innerHTML = element[prop];

            field.appendChild(attribute);
            field.appendChild(content);

            ol.appendChild(field);
          }

          elem.appendChild(ol);
          documents.appendChild(elem);
        }

        // When we click on a facet value we change the global values
        // to make sure that we don't loose the selection between requests.
        $('#facets select').on('change', function(e) {
            let facet_name = $(this).attr('data-facet-name');
            selected_facets[facet_name] = $(this).val();
            $('#query').trigger('input');
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
