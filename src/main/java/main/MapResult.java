package main;



import java.util.*;
import com.au.ds.model.Restaurant;

/**
 * Resultado parcial que devuelve cada Worker:
 * - Lista de restaurantes que cumplen los filtros
 * - Mapa de ventas parciales (storeName→unidades vendidas) para reportes
 *
 * Para búsquedas: se usa solo restaurants;
 * para reportes de ventas: se usa ventasPorKey (FoodCategory o ProductCategory).
 */
public class MapResult {
    private final List<Restaurant> restaurants = new ArrayList<>();
    private final Map<String, Integer> ventasPorKey = new HashMap<>();

    /** Añade un restaurante a los resultados de búsqueda. */
    public void addRestaurant(Restaurant r) {
        restaurants.add(r);
    }

    /** Suma unidades vendidas para la clave dada (store o producto). */
    public void addVenta(String key, int unidades) {
        ventasPorKey.merge(key, unidades, Integer::sum);
    }

    /** Fusiona otro MapResult en este (para el paso de reduce). */
    public void merge(MapResult other) {
        this.restaurants.addAll(other.restaurants);
        other.ventasPorKey.forEach(
            (k,v) -> this.ventasPorKey.merge(k, v, Integer::sum)
        );
    }

    public List<Restaurant> getRestaurants() {
        return Collections.unmodifiableList(restaurants);
    }
    public Map<String, Integer> getVentasPorKey() {
        return Collections.unmodifiableMap(ventasPorKey);
    }
}

