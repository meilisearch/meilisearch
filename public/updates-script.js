$(window).on('load', function () {
  let url = 'ws://' + window.location.hostname + ':' + window.location.port + '/updates/ws';
  var socket = new WebSocket(url);

  socket.onmessage = function (event) {
    console.log(event.data);

    if (event.data.endsWith("processed")) {
      const elem = document.createElement('li');
      elem.classList.add("document");

      const ol = document.createElement('ol');
      const field = document.createElement('li');
      field.classList.add("field");

      const attribute = document.createElement('div');
      attribute.classList.add("attribute");
      attribute.innerHTML = "TEXT";

      const content = document.createElement('div');
      content.classList.add("content");
      content.innerHTML = event.data;

      field.appendChild(attribute);
      field.appendChild(content);

      ol.appendChild(field);
      elem.appendChild(ol);

      prependChild(results, elem);
    }
  }
});

function prependChild(parent, newFirstChild) {
  parent.insertBefore(newFirstChild, parent.firstChild)
}
