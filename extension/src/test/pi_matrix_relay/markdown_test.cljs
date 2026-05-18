(ns pi-matrix-relay.markdown-test
  (:require [cljs.test :refer [deftest is]]
            [clojure.string :as str]
            [pi-matrix-relay.markdown :as markdown]))

(deftest renders-basic-gfm-markdown-as-matrix-html
  (is (= "<h2>Title</h2>\n<p>Hello <strong>bold</strong>, <em>em</em>, <del>old</del>, and <code>x &lt; y</code>.</p>"
         (markdown/markdown->matrix-html "## Title\n\nHello **bold**, *em*, ~~old~~, and `x < y`."))))

(deftest renders-gfm-tables-without-unsupported-alignment-attributes
  (let [html (markdown/markdown->matrix-html "| Name | Value |\n| :--- | ---: |\n| **A** | 1 |")]
    (is (= "<table>\n<thead>\n<tr>\n<th>Name</th>\n<th>Value</th>\n</tr>\n</thead>\n<tbody><tr>\n<td><strong>A</strong></td>\n<td>1</td>\n</tr>\n</tbody></table>"
           html))
    (is (not (str/includes? html "align=")))))

(deftest escapes-raw-html-instead-of-passing-it-through
  (let [html (markdown/markdown->matrix-html "<script>alert(1)</script>\n\nRaw <b>bold</b>")]
    (is (str/includes? html "&lt;script&gt;alert(1)&lt;/script&gt;"))
    (is (str/includes? html "Raw &lt;b&gt;bold&lt;/b&gt;"))
    (is (not (str/includes? html "<script")))
    (is (not (str/includes? html "<b>")))))

(deftest allows-only-matrix-safe-absolute-link-schemes
  (is (= "<p><a href=\"https://example.org?a=1&amp;b=2\">https</a> <a href=\"http://example.org\">http</a> <a href=\"ftp://example.org/file\">ftp</a> <a href=\"mailto:ops@example.org\">mail</a> <a href=\"magnet:?xt=urn:btih:abcdef\">magnet</a></p>"
         (markdown/markdown->matrix-html "[https](https://example.org?a=1&amp;b=2) [http](http://example.org) [ftp](ftp://example.org/file) [mail](mailto:ops@example.org) [magnet](magnet:?xt=urn:btih:abcdef)")))
  (is (= "<p>relative protocol javascript tel sms</p>"
         (markdown/markdown->matrix-html "[relative](/docs) [protocol](//example.org) [javascript](j&#97;vascript:alert(1)) [tel](tel:+123) [sms](sms:+123)"))))

(deftest renders-images-and-task-checkboxes-without-unsupported-tags
  (let [html (markdown/markdown->matrix-html "![alt <tag>](https://example.org/image.png)\n\n- [x] done\n- [ ] todo")]
    (is (= "<p>alt &lt;tag&gt;</p>\n<ul>\n<li>[x] done</li>\n<li>[ ] todo</li>\n</ul>"
           html))
    (is (not (str/includes? html "<img")))
    (is (not (str/includes? html "<input")))))

(deftest sanitizes-code-language-class
  (let [html (markdown/markdown->matrix-html "```clj\" onclick=\"x\n(+ 1 2)\n```")]
    (is (= "<pre><code class=\"language-cljonclickx\">(+ 1 2)\n</code></pre>"
           html))
    (is (not (str/includes? html "onclick=")))
    (is (not (str/includes? html "language-clj\"")))))
