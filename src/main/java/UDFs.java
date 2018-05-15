import lombok.val;
import org.apache.spark.sql.api.java.UDF1;

import java.time.format.DateTimeFormatter;

import static java.time.LocalDateTime.parse;
import static java.time.format.DateTimeFormatter.ofPattern;

public class UDFs {

    public static final Double MIN_TEMP = -40D;
    public static final Double MAX_TEMP = 44D;

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
            (UDF1<String, Integer>) timestamp -> {
                val date =  parse(timestamp, TIMESTAMP_FORMATTER);
                val dayHour = date.getHour();

                if (dayHour < 6)
                    return 1;
                if (dayHour >= 6 && dayHour < 10)
                    return 2;
                if (dayHour >= 10 && dayHour <= 19)
                    return 3;

                return 4;
            };

    public static final UDF1 normalizeIndoorTemp =
            (UDF1<Double, Double>) indoorTemp -> (indoorTemp - MIN_TEMP)/(MAX_TEMP - MIN_TEMP);

    public static final UDF1 normalizeOutdoorTemp =
            (UDF1<Double, Double>) outoorTemp -> (outoorTemp - MIN_TEMP)/(MAX_TEMP - MIN_TEMP);

    public static final UDF1 disnornalizeIndoorTemp =
            (UDF1<Double, Double>) normIndoor -> normIndoor*(MAX_TEMP - MIN_TEMP) + MIN_TEMP;

}
