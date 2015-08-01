(function() {
  var ContentDiv = this.ContentDiv = function(dom) {
    that = this;
    this.dom = dom;

    $(window).hashchange(function(){
      that.onHashChange();
    });

    var target = 'documentation/latest/home/';
    if (window.location.hash.length > 1 &&
        window.location.hash.startsWith('#')) {

      target = window.location.hash.slice(1);
    }
    this.load(target);
    this.onHashChange();
  };

  ContentDiv.prototype = {
    convertToUrl: function(target) {
      var prefix = 'https://raw.githubusercontent.com/haosdent/mesos/master/';
      var docPrefix = 'documentation/latest/';
      var url = '';
      if (target.startsWith(docPrefix)) {
        target = target.slice(docPrefix.length)
        if (target.endsWith('/')) {
          target = target.slice(0, -1)
        }
        url = prefix + 'docs/' + target + '.md';
      } else if (target.endsWith('.md')) {
        url = prefix + 'docs/' + target;
      }
      return url;
    },
    load: function(target) {
      this.target = target;
      window.location.hash = this.target;
    },
    onHashChange: function() {
      var that = this;
      var target = window.location.hash.slice(1);
      var url = this.convertToUrl(target);
      $.get(url, function(data) {
        that.dom.innerHTML = that.convertMarkdownToHtml(data);
        $('a').off('click');
        $('a').click(function(evt) {
          that.handleLink(evt);
        });
      });
    },
    convertMarkdownToHtml: function(md) {
      var layoutPrefix = '---\nlayout: documentation\n---';
      if (md.startsWith(layoutPrefix)) {
        md = md.slice(layoutPrefix.length);
      }
      var html = marked(md);
      return html;
    },
    isInternalLink: function(link) {
      return link && link.startsWith(window.location.origin);
    },
    handleLink: function(evt) {
      var href = evt.target.href;
      if (!this.isInternalLink(href)) {
        return;
      }

      evt.preventDefault();
      var target = href.slice(window.location.origin.length + 1);
      this.load(target);
    }
  };

  this.contentDiv = new ContentDiv(document.getElementById('content'));

}).call(window);