(ns pi-matrix-relay.markdown
  (:require [clojure.string :as str]
            ["marked" :as marked]))

(def ^:private allowed-link-protocols
  #{"http:" "https:" "ftp:" "mailto:" "magnet:"})

(defn- replacement-match
  [match]
  (if (vector? match) (first match) match))

(defn- html-escape-replacement
  [match]
  (case (replacement-match match)
    "&" "&amp;"
    "<" "&lt;"
    ">" "&gt;"
    "\"" "&quot;"
    "'" "&#39;"
    (replacement-match match)))

(defn- html-escape
  ([value]
   (html-escape value false))
  ([value encode-existing-entities?]
   (let [text (str value)]
     (if encode-existing-entities?
       (str/replace text #"[&<>\"']" html-escape-replacement)
       (str/replace text #"[<>\"']|&(?!(#\d{1,7}|#[Xx][a-fA-F0-9]{1,6}|\w+);)" html-escape-replacement)))))

(def ^:private named-url-entities
  {"amp" "&"
   "apos" "'"
   "colon" ":"
   "gt" ">"
   "lt" "<"
   "quot" "\""})

(defn- code-point->string
  [code-point fallback]
  (if (or (js/Number.isNaN code-point)
          (neg? code-point)
          (> code-point 0x10ffff))
    fallback
    (js/String.fromCodePoint code-point)))

(defn- decode-html-entity-match
  [match]
  (let [full-match (first match)
        entity (second match)
        normalized (str/lower-case entity)]
    (cond
      (str/starts-with? normalized "#x")
      (code-point->string (js/parseInt (subs normalized 2) 16) full-match)

      (str/starts-with? normalized "#")
      (code-point->string (js/parseInt (subs normalized 1) 10) full-match)

      :else
      (get named-url-entities normalized full-match))))

(defn- decode-html-entities
  [value]
  (str/replace (str value)
               #"&(#x[0-9a-fA-F]+|#[0-9]+|[a-zA-Z][a-zA-Z0-9]+);?"
               decode-html-entity-match))

(defn- unsafe-url-char?
  [ch]
  (let [code (.charCodeAt ch 0)]
    (or (<= code 0x20)
        (and (>= code 0x7f) (<= code 0x9f))
        (and (>= code 0x200c) (<= code 0x200f))
        (= code 0xfeff))))

(defn- safe-link-href
  [href]
  (let [decoded (str/trim (decode-html-entities (or href "")))]
    (when (and (not (str/blank? decoded))
               (not (some unsafe-url-char? decoded)))
      (try
        (let [url (js/URL. decoded)
              protocol (.-protocol url)]
          (when (contains? allowed-link-protocols protocol)
            decoded))
        (catch :default _
          nil)))))

(defn- sanitize-language
  [lang]
  (let [sanitized (some-> (or lang "")
                          (str/replace #"[^A-Za-z0-9_-]" ""))]
    (when-not (str/blank? sanitized)
      sanitized)))

(defn- parse-block
  [^js renderer tokens]
  (let [^js parser (.-parser renderer)]
    (.parse parser tokens)))

(defn- parse-inline
  [^js renderer tokens]
  (let [^js parser (.-parser renderer)]
    (.parseInline parser tokens)))

(defn- js-array->seq
  [value]
  (when value
    (array-seq value)))

(defn- render-table-row
  [content]
  (str "<tr>\n" content "</tr>\n"))

(defn- unwrap-single-paragraph
  [html]
  (if-let [[_ content] (re-matches #"(?s)<p>(.*)</p>\n?" html)]
    content
    html))

(defn- render-table-cell
  [^js renderer ^js token]
  (let [tag (if (.-header token) "th" "td")]
    (str "<" tag ">"
         (parse-inline renderer (.-tokens token))
         "</" tag ">\n")))

(defn- make-renderer
  []
  (let [renderer (marked/Renderer.)]
    (set! (.-space renderer)
          (fn [_token]
            ""))
    (set! (.-code renderer)
          (fn [^js token]
            (let [text (str/replace (or (.-text token) "") #"\n$" "")
                  language (sanitize-language (.-lang token))
                  class-attr (when language
                               (str " class=\"language-" (html-escape language true) "\""))]
              (str "<pre><code" class-attr ">"
                   (html-escape (str text "\n") true)
                   "</code></pre>"))))
    (set! (.-blockquote renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this]
                (str "<blockquote>\n" (parse-block this (.-tokens token)) "</blockquote>\n")))))
    (set! (.-html renderer)
          (fn [^js token]
            (html-escape (.-text token))))
    (set! (.-heading renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this
                    depth (min 6 (max 1 (.-depth token)))]
                (str "<h" depth ">" (parse-inline this (.-tokens token)) "</h" depth ">\n")))))
    (set! (.-hr renderer)
          (fn [_token]
            "<hr>\n"))
    (set! (.-list renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this
                    ordered? (.-ordered token)
                    tag (if ordered? "ol" "ul")
                    start (.-start token)
                    start-attr (when (and ordered?
                                           (number? start)
                                           (not= 1 start))
                                 (str " start=\"" start "\""))
                    body (->> (js-array->seq (.-items token))
                              (map #((.-listitem this) %))
                              (apply str))]
                (str "<" tag start-attr ">\n" body "</" tag ">\n")))))
    (set! (.-listitem renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this
                    body (parse-block this (.-tokens token))
                    task? (.-task token)
                    prefix (when task?
                             (if (.-checked token) "[x] " "[ ] "))]
                (str "<li>" prefix (if task? (unwrap-single-paragraph body) body) "</li>\n")))))
    (set! (.-checkbox renderer)
          (fn [^js token]
            (if (.-checked token) "[x] " "[ ] ")))
    (set! (.-paragraph renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this]
                (str "<p>" (parse-inline this (.-tokens token)) "</p>\n")))))
    (set! (.-table renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this
                    header (->> (js-array->seq (.-header token))
                                (map #(render-table-cell this %))
                                (apply str)
                                render-table-row)
                    body (->> (js-array->seq (.-rows token))
                              (map (fn [row]
                                     (->> (js-array->seq row)
                                          (map #(render-table-cell this %))
                                          (apply str)
                                          render-table-row)))
                              (apply str))]
                (str "<table>\n<thead>\n"
                     header
                     "</thead>\n"
                     (when (seq body)
                       (str "<tbody>" body "</tbody>"))
                     "</table>\n")))))
    (set! (.-tablerow renderer)
          (fn [^js token]
            (render-table-row (.-text token))))
    (set! (.-tablecell renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this]
                (render-table-cell this token)))))
    (set! (.-strong renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this]
                (str "<strong>" (parse-inline this (.-tokens token)) "</strong>")))))
    (set! (.-em renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this]
                (str "<em>" (parse-inline this (.-tokens token)) "</em>")))))
    (set! (.-codespan renderer)
          (fn [^js token]
            (str "<code>" (html-escape (.-text token) true) "</code>")))
    (set! (.-br renderer)
          (fn [_token]
            "<br>"))
    (set! (.-del renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this]
                (str "<del>" (parse-inline this (.-tokens token)) "</del>")))))
    (set! (.-link renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this
                    label (parse-inline this (.-tokens token))]
                (if-let [href (safe-link-href (.-href token))]
                  (str "<a href=\"" (html-escape href true) "\">" label "</a>")
                  label)))))
    (set! (.-image renderer)
          (fn [^js token]
            (html-escape (or (.-text token) ""))))
    (set! (.-text renderer)
          (fn [^js token]
            (this-as this
              (let [^js this this]
                (if (and (js-in "tokens" token) (.-tokens token))
                (parse-inline this (.-tokens token))
                  (html-escape (.-text token)))))))
    renderer))

(defn markdown->matrix-html
  [text]
  (let [input (or text "")]
    (if (str/blank? input)
      ""
      (let [renderer (make-renderer)
            parser (marked/Marked. #js {:gfm true
                                        :breaks false
                                        :renderer renderer})]
        (str/trim (.parse parser input))))))
