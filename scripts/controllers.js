(function() {
  var ContentDiv = this.ContentDiv = function(dom) {
    this.dom = dom;

    var target = 'wiki/README.md';
    if (window.location.hash.startsWith('#wiki/')) {
      target = window.location.hash.slice(1);
    }
    this.load(target);
  };

  ContentDiv.prototype = {
    load: function(target) {
      this.target = target;
      window.location.hash = this.target;
      var that = this;
      $.get(this.target, function(data) {
        that.dom.innerHTML = that.convertMarkdownToHtml(data);
        $('a').click(function(evt) {
          that.handleLink(evt);
        });
      });
    },
    convertMarkdownToHtml: function(md) {
      var html = marked(md);
      return html;
    },
    handleLink: function(evt) {
      var href = evt.target.href;
      var prefix = 'https://github.com/goagent/goagent/blob/wiki/';
      if (href && href.startsWith(prefix)) {
        evt.preventDefault();
        var target = 'wiki/' + href.slice(prefix.length);
        this.load(target);
      }
    }
  };

  this.contentDiv = new ContentDiv(document.getElementById('content'));
}).call(window);