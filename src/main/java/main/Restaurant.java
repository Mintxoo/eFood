package main;



public class Restaurant {
    private String name;
    private double latitude, longitude;
    private String foodCategory;
    private int stars;
    private PriceCategory priceCategory;

    public Restaurant() {}

    public Restaurant(String name, double latitude, double longitude,
                      String foodCategory, int stars, PriceCategory priceCategory) {
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
        this.foodCategory = foodCategory;
        this.stars = stars;
        this.priceCategory = priceCategory;
    }

    // getters
    public String getName() { return name; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public String getFoodCategory() { return foodCategory; }
    public int getStars() { return stars; }
    public PriceCategory getPriceCategory() { return priceCategory; }

    @Override
    public String toString() {
        return "Restaurant{" +
               "name='" + name + '\'' +
               ", lat=" + latitude +
               ", lon=" + longitude +
               ", cat='" + foodCategory + '\'' +
               ", stars=" + stars +
               ", price=" + priceCategory +
               '}';
    }
}
