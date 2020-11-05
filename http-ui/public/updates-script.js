$(window).on('load', function () {
  let wsProtcol = "ws";
  if (window.location.protocol === 'https') {
    wsProtcol = 'wss';
  }

  let url = wsProtcol + '://' + window.location.hostname + ':' + window.location.port + '/updates/ws';
  var socket = new WebSocket(url);

  socket.onmessage = function (event) {
    let status = JSON.parse(event.data);

    if (status.type == 'Pending') {
      const elem = document.createElement('li');
      elem.classList.add("document");
      elem.setAttribute("id", 'update-' + status.update_id);

      const ol = document.createElement('ol');
      const field = document.createElement('li');
      field.classList.add("field");

      const attributeUpdateId = document.createElement('div');
      attributeUpdateId.classList.add("attribute");
      attributeUpdateId.innerHTML = "update id";

      const contentUpdateId = document.createElement('div');
      contentUpdateId.classList.add("updateId");
      contentUpdateId.classList.add("content");
      contentUpdateId.innerHTML = status.update_id;

      field.appendChild(attributeUpdateId);
      field.appendChild(contentUpdateId);

      const attributeUpdateStatus = document.createElement('div');
      attributeUpdateStatus.classList.add("attribute");
      attributeUpdateStatus.innerHTML = "update status";

      const contentUpdateStatus = document.createElement('div');
      contentUpdateStatus.classList.add("updateStatus");
      contentUpdateStatus.classList.add("content");
      contentUpdateStatus.innerHTML = 'pending';

      field.appendChild(attributeUpdateStatus);
      field.appendChild(contentUpdateStatus);

      ol.appendChild(field);
      elem.appendChild(ol);

      prependChild(results, elem);
    }

    if (status.type == "Progressing") {
      const id = 'update-' + status.update_id;
      const content = $(`#${id} .updateStatus.content`);
      content.html('progressing... ' + JSON.stringify(status.meta));
    }

    if (status.type == "Processed") {
      const id = 'update-' + status.update_id;
      const content = $(`#${id} .updateStatus.content`);
      content.html('processed ' + JSON.stringify(status.meta));
    }
  }
});

function prependChild(parent, newFirstChild) {
  parent.insertBefore(newFirstChild, parent.firstChild)
}

// Make the number of document a little bit prettier
$('#docs-count').text(function(index, text) {
  return parseInt(text).toLocaleString()
});

// Make the database a little bit easier to read
$('#db-size').text(function(index, text) {
  return filesize(parseInt(text))
});
