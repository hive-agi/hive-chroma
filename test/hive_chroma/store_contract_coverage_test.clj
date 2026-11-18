(ns hive-chroma.store-contract-coverage-test
  "Chroma adapter contract coverage and schema-derived field-key facets."
  (:require [hive-chroma.store :as chroma]
            [hive-schemas.test :as hst]))

(hst/deftrifecta-from-schema field-key-contract
  hive-chroma.store/field-key
  {:in [:enum "project_id" "foo_bar" "id" :project-id :some-field]
   :out :keyword
   :rel (fn [in out]
          (= (name out)
             (clojure.string/replace (if (instance? clojure.lang.Named in)
                                       (name in) (str in)) "_" "-")))
   :golden-path "test/golden/hive-chroma/field-key-contract.edn"
   :num-tests 100})

(hst/deftrifecta-from-schema project-fields-contract
  hive-chroma.store/project-fields
  {:in [:cat [:vector [:map [:id :string] [:project-id :string] [:content :string]]]
        [:vector [:enum "project_id" "content" "missing"]]]
   :out [:vector :map]
   :rel (fn [[entries fields] out]
          (and (= (count entries) (count out))
               (every? #(contains? % :id) out)
               (every? #(every? (if (seq fields)
                                 (set (concat [:id] (map chroma/field-key fields)))
                                 (set (mapcat keys entries)))
                                 (keys %)) out)))
   :golden-path "test/golden/hive-chroma/project-fields-contract.edn"
   :num-tests 100})
