package main;



import com.au.ds.model.Restaurant;

import java.util.*;

/**
 * Combina varios MapResult en el resultado final.
 */
public class ReduceTask {

    /**
     * Junta todos los MapResult parciales en uno solo:
     * - concatena listas de restaurantes
     * - suma ventas por clave (store o categoría)
     */
    public ReduceResult combine(List<MapResult> partials) {
        List<Restaurant> allRestaurants = new ArrayList<>();
        Map<String, Integer> ventas = new HashMap<>();

        for (MapResult pr : partials) {
            // 1) agregar restaurantes
            allRestaurants.addAll(pr.getRestaurants());
            // 2) sumar ventas por clave
            pr.getVentasPorKey().forEach(
                (key, unidades) -> ventas.merge(key, unidades, Integer::sum)
            );
        }

        // 3) construir el resultado final
        return new ReduceResult(allRestaurants, ventas);
    }
}

