/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package de.m3y3r.dupfilefinder.avro;  
@SuppressWarnings("all")
/** file size/path index entry */
@org.apache.avro.specific.AvroGenerated
public class SizePathAvro extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"SizePathAvro\",\"namespace\":\"de.m3y3r.dupfilefinder.avro\",\"doc\":\"file size/path index entry\",\"fields\":[{\"name\":\"fileSize\",\"type\":\"long\"},{\"name\":\"filePath\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public long fileSize;
  @Deprecated public java.lang.CharSequence filePath;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use {@link \#newBuilder()}. 
   */
  public SizePathAvro() {}

  /**
   * All-args constructor.
   */
  public SizePathAvro(java.lang.Long fileSize, java.lang.CharSequence filePath) {
    this.fileSize = fileSize;
    this.filePath = filePath;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return fileSize;
    case 1: return filePath;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: fileSize = (java.lang.Long)value$; break;
    case 1: filePath = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'fileSize' field.
   */
  public java.lang.Long getFileSize() {
    return fileSize;
  }

  /**
   * Sets the value of the 'fileSize' field.
   * @param value the value to set.
   */
  public void setFileSize(java.lang.Long value) {
    this.fileSize = value;
  }

  /**
   * Gets the value of the 'filePath' field.
   */
  public java.lang.CharSequence getFilePath() {
    return filePath;
  }

  /**
   * Sets the value of the 'filePath' field.
   * @param value the value to set.
   */
  public void setFilePath(java.lang.CharSequence value) {
    this.filePath = value;
  }

  /** Creates a new SizePathAvro RecordBuilder */
  public static de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder newBuilder() {
    return new de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder();
  }
  
  /** Creates a new SizePathAvro RecordBuilder by copying an existing Builder */
  public static de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder newBuilder(de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder other) {
    return new de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder(other);
  }
  
  /** Creates a new SizePathAvro RecordBuilder by copying an existing SizePathAvro instance */
  public static de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder newBuilder(de.m3y3r.dupfilefinder.avro.SizePathAvro other) {
    return new de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder(other);
  }
  
  /**
   * RecordBuilder for SizePathAvro instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<SizePathAvro>
    implements org.apache.avro.data.RecordBuilder<SizePathAvro> {

    private long fileSize;
    private java.lang.CharSequence filePath;

    /** Creates a new Builder */
    private Builder() {
      super(de.m3y3r.dupfilefinder.avro.SizePathAvro.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.fileSize)) {
        this.fileSize = data().deepCopy(fields()[0].schema(), other.fileSize);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.filePath)) {
        this.filePath = data().deepCopy(fields()[1].schema(), other.filePath);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing SizePathAvro instance */
    private Builder(de.m3y3r.dupfilefinder.avro.SizePathAvro other) {
            super(de.m3y3r.dupfilefinder.avro.SizePathAvro.SCHEMA$);
      if (isValidValue(fields()[0], other.fileSize)) {
        this.fileSize = data().deepCopy(fields()[0].schema(), other.fileSize);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.filePath)) {
        this.filePath = data().deepCopy(fields()[1].schema(), other.filePath);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'fileSize' field */
    public java.lang.Long getFileSize() {
      return fileSize;
    }
    
    /** Sets the value of the 'fileSize' field */
    public de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder setFileSize(long value) {
      validate(fields()[0], value);
      this.fileSize = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'fileSize' field has been set */
    public boolean hasFileSize() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'fileSize' field */
    public de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder clearFileSize() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'filePath' field */
    public java.lang.CharSequence getFilePath() {
      return filePath;
    }
    
    /** Sets the value of the 'filePath' field */
    public de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder setFilePath(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.filePath = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'filePath' field has been set */
    public boolean hasFilePath() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'filePath' field */
    public de.m3y3r.dupfilefinder.avro.SizePathAvro.Builder clearFilePath() {
      filePath = null;
      fieldSetFlags()[1] = false;
      return this;
    }

//    @Override
    public SizePathAvro build() {
      try {
        SizePathAvro record = new SizePathAvro();
        record.fileSize = fieldSetFlags()[0] ? this.fileSize : (java.lang.Long) defaultValue(fields()[0]);
        record.filePath = fieldSetFlags()[1] ? this.filePath : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}