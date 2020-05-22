/**
 * (source datartisans)
 */

package dfki.data;

import java.util.Date;
import java.text.SimpleDateFormat;

public class KeyedDataPoint<T> extends DataPoint<T> {

  private String key;

  public KeyedDataPoint(){
    super();
    this.key = null;
  }

  public KeyedDataPoint(String key, long timeStampMs, T value) {
    super(timeStampMs, value);
    this.key = key;
  }

  @Override
  public String toString() {
    Date date = new Date(getTimeStampMs());
    // to print the date in normal format
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    return  sdf.format(date) + ","+ key + "," + getValue();

    // to print the date as epoch
    // return getTimeStampMs() + "," + getKey() + "," + getValue();
    //return getTimeStampMs() + "," + getValue();
  }


  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public <R> KeyedDataPoint<R> withNewValue(R newValue){
    return new KeyedDataPoint<>(this.getKey(), this.getTimeStampMs(), newValue);
  }

}
