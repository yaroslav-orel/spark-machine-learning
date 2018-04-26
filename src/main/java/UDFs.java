import lombok.val;
import org.apache.spark.sql.api.java.UDF1;

import java.time.format.DateTimeFormatter;

import static java.time.LocalDateTime.parse;
import static java.time.format.DateTimeFormatter.ofPattern;

public class UDFs {
    // min max for minites
    private static final Double minIndoorTemp = 10D;
    private static final Double maxIndoorTemp = 35.27D;
    private static final Double minOutdoorTemp = -6.25D;
    private static final Double maxOutdoorTemp = 32.14D;

    //min max for full set
    /*private static final Double minIndoorTemp = 10D;
    private static final Double maxIndoorTemp = 41.483D;
    private static final Double minOutdoorTemp = -20.43D;
    private static final Double maxOutdoorTemp = 38.716D;*/

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = ofPattern("yyyy-MM-dd'T'HH:mm");

    public static final UDF1 timestampToWeekDay =
            (UDF1<String, Integer>) timestamp -> parse(timestamp, TIMESTAMP_FORMATTER).getDayOfWeek().getValue();

    public static final UDF1 timestampToDate =
            (UDF1<String, String>) timestamp -> {
                val date =  parse(timestamp, TIMESTAMP_FORMATTER);
                val dateFormatter = ofPattern("yyyy-MM-dd");
                return date.format(dateFormatter);
            };

    public static final UDF1 timestampToDayPeriod =
            (UDF1<String, Integer>) timestamp -> (int) (parse(timestamp, TIMESTAMP_FORMATTER).getHour() / 6.0D) + 1;

    public static final UDF1 normalizeIndoorTemp =
            (UDF1<Double, Double>) indoorTemp -> (indoorTemp - minIndoorTemp)/(maxIndoorTemp - minIndoorTemp);

    public static final UDF1 normalizeOutdoorTemp =
            (UDF1<Double, Double>) outoorTemp -> (outoorTemp - minOutdoorTemp)/(maxOutdoorTemp - minOutdoorTemp);

}