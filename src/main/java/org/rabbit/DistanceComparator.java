package org.rabbit;

import java.awt.geom.Point2D;
import java.util.Comparator;

public class DistanceComparator implements Comparator<QueryMatch> {
    Point2D orign;

    public DistanceComparator(double lon, double lat){
        this.orign = new Point2D.Double(lon,lat );
    }

    @Override
    public int compare(QueryMatch q1, QueryMatch q2) {
        double d1 = orign.distance(q1.lon,q1.lat);
        double d2 = orign.distance(q2.lon,q2.lat);
        return java.lang.Double.compare(d1,d2);
    }
}
