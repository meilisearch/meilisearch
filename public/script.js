var request = null;

$('#search').on('input', function () {
  var query = $(this).val();
  request = $.ajax({
    type: "POST",
    url: "query",
    contentType: 'application/json',
    data: JSON.stringify({ 'query': query }),
    contentType: 'application/json',
    success: function (data, textStatus, request) {
      let httpResults = Papa.parse(data, { header: true, skipEmptyLines: true });
      results.innerHTML = '';

      let timeSpent = request.getResponseHeader('Time-Ms');
      let numberOfDocuments = httpResults.data.length;
      count.innerHTML = `${numberOfDocuments}`;
      time.innerHTML = `${timeSpent}ms`;

      for (element of httpResults.data) {
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
        results.appendChild(elem)
      }

    },
    beforeSend: function () {
      if (request !== null) {
        request.abort();
      }
    },
  });
});
