var request = null;
var timeoutID = null;

$('#query, #facet').on('input', function () {
  var query = $('#query').val();
  var facet = $('#facet').val();
  let fetchFacetDistribution = query.trim() !== "" || facet.trim() !== "";
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
        'query': query, 'facetCondition': facet, "facetDistribution": fetchFacetDistribution
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
          for (value of data.facets[facet_name]) {
              const elem = document.createElement('span');
              elem.classList.add("tag");
              elem.setAttribute('data-name', facet_name);
              elem.setAttribute('data-value', value);
              elem.innerHTML = `${facet_name}:${value}`;
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
            content.innerHTML = element[prop];

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

          let facet_query = $('#facet').val().trim();
          if (facet_query === "") {
            $('#facet').val(`${name} = "${value}"`).trigger('input');
          } else {
            $('#facet').val(`${facet_query} AND ${name} = "${value}"`).trigger('input');
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
