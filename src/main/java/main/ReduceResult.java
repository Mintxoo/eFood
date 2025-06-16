package main;



import java.util.*;
import com.au.ds.model.Restaurant;

/**
 * Resultado final del Master tras combinar todos los MapResult:
 * - Lista consolidada de restaurantes (ordenada si se desea)
 * - Mapa de ventas agregadas (incluye clave "total" con suma global)
 */
public class ReduceResult {
    private final List<Restaurant> restaurants;
    private final Map<String, Integer> ventasPorKey;

    public ReduceResult(List<Restaurant> restaurants, Map<String, Integer> ventasPorKey) {
        this.restaurants = new ArrayList<>(restaurants);
        // crear copia defensiva e incluir total ventas si procede
        Map<String, Integer> copy = new HashMap<>(ventasPorKey);
        if (!ventasPorKey.isEmpty()) {
            int total = ventasPorKey.values().stream().mapToInt(Integer::intValue).sum();
            copy.put("total", total);
        }
        this.ventasPorKey = Collections.unmodifiableMap(copy);
    }

    public List<Restaurant> getRestaurants() {
        return Collections.unmodifiableList(restaurants);
    }

    /**
     * Ventas por FoodCategory o ProductCategory, incluye clave "total"
     * con la suma de todas las ventas parciales.
     */
    public Map<String, Integer> getVentasPorKey() {
        return ventasPorKey;
    }
}

