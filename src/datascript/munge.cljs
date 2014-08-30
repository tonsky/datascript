(ns datascript.munge
  (:require
   [clojure.string :as string]))

(def char-map
  {"-" "_"
   ":" "_COLON_"
   "+" "_PLUS_"
   ">" "_GT_"
   "<" "_LT_"
   "=" "_EQ_"
   "~" "_TILDE_"
   "!" "_BANG_"
   "@" "_CIRCA_"
   "#" "_SHARP_"
   "'" "_SINGLEQUOTE_"
   "\"" "_DOUBLEQUOTE_"
   "%" "_PERCENT_"
   "^" "_CARET_"
   "&" "_AMPERSAND_"
   "*" "_STAR_"
   "|" "_BAR_"
   "{" "_LBRACE_"
   "}" "_RBRACE_"
   "[" "_LBRACK_"
   "]" "_RBRACK_"
   "/" "_SLASH_"
   "\\" "_BSLASH_"
   "?" "_QMARK_"})

(defn munge
  [sym]
  (reduce
   (fn [val [char replacement]]
     (string/replace val char replacement))
   (str sym)
   char-map))
