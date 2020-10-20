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

      const attribute = document.createElement('div');
      attribute.classList.add("attribute");
      attribute.innerHTML = "TEXT";

      const content = document.createElement('div');
      content.classList.add("content");
      content.innerHTML = 'Pending ' + status.update_id;

      field.appendChild(attribute);
      field.appendChild(content);

      ol.appendChild(field);
      elem.appendChild(ol);

      prependChild(results, elem);
    }

    if (status.type == "Processing") {
      const id = 'update-' + status.update_id;
      const content = $(`#${id} .content`);
      content.html('Processing ' + status.update_id);
    }

    if (status.type == "Processed") {
      const id = 'update-' + status.update_id;
      const content = $(`#${id} .content`);
      content.html('Processed ' + status.update_id);
    }
  }
});

function prependChild(parent, newFirstChild) {
  parent.insertBefore(newFirstChild, parent.firstChild)
}
