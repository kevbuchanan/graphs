(ns flight-data.core
  (:require [edgewise.core :as e]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.data.csv :as csv]))

(defn csv->data [file]
  (-> file
      io/resource
      io/reader
      csv/read-csv))

(defn data->edges [data]
  (map #(vector (nth % 2) (nth % 4)) data))

(defn edges->weighted-edges [edges]
  (sort-by val > (frequencies edges)))

(defn edges->graph [edges]
  (reduce
    (fn [g [[i o] f]]
      (e/add-edge g i o {:weight f}))
    (e/empty-graph)
    edges))

(defn build-graph [file-name]
  (-> file-name
      csv->data
      data->edges
      edges->weighted-edges
      edges->graph))

(defn distinct-incoming-routes [g codes]
  (let [ids (map (partial e/label-index g) codes)
        t (e/traversal g ids [])]
    (-> t
        e/inE
        e/outV
        e/props
        count)))

(defn origins-and-freqs [g codes]
  (let [ids (map (partial e/label-index g) codes)
        t (e/traversal g ids [])
        edges (-> t e/inE (e/props :outV :weight))]
    (map
      (fn [[id w]]
        {:origin (-> (e/v g id) (e/props :label) ffirst)
         :freq w})
      edges)))

(defn degree-distribution [g]
  (let [v-ids (keys (:vertex-data g))]
    (pmap
      (fn [id]
        {:code (-> (e/v g id) (e/props :label) ffirst)
         :out (-> (e/traversal g [id] []) e/outE e/props count)
         :in (-> (e/traversal g [id] []) e/inE e/props count)})
      v-ids)))

(defn in-degree-distribution-freq [g]
  (->> (pmap :in (degree-distribution g))
       frequencies
       (map (fn [[k v]] {:in k :count v}))
       (sort-by :count >)))

(defn question
  ([q headers a]
    (println (str q ":"))
    (pprint/print-table headers a)
    (println))
  ([q a]
    (println (format (str q ":\n\n%s\n") a))))

(defn -main [& args]
  (let [g (build-graph "routes.csv")]
    (question "What is the incoming Chicago routes count?"
      (distinct-incoming-routes g ["ORD" "MDW"]))

    (question "What are the origins of incoming Chicago routes and frequencies?"
      [:origin :freq]
      (take 20 (sort-by :freq > (origins-and-freqs g ["ORD" "MDW"]))))

    (question "What is the degree distribution of this network?"
      [:code :in :out]
      (take 20 (sort-by :in > (degree-distribution g))))

    (question "What is the in-degree distribution frequency of this network?"
      [:in :count]
      (take 20 (in-degree-distribution-freq g))))

  (shutdown-agents))
