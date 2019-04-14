package it.castielvr.challenge.itemsales.utils

object CollectionUtils {
  def mergeMaps[K, V](map1: Map[K, V], map2: Map[K, V], compoOp: (V, V) => V): Map[K, V] = {
    val keys1 = map1.keys.toSeq
    map1.map {
      case (k1, v1) =>
        val newValue = map2.get(k1).map(v2 => compoOp(v1, v2)).getOrElse(v1)
        k1 -> newValue
    } ++
      map2.filterNot {
        case (k2, v2) => keys1.contains(k2)
      }
  }
}
