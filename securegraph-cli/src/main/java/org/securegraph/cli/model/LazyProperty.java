package org.securegraph.cli.model;

import org.apache.commons.io.IOUtils;
import org.securegraph.Metadata;
import org.securegraph.Property;
import org.securegraph.Visibility;
import org.securegraph.cli.SecuregraphScript;
import org.securegraph.property.StreamingPropertyValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

public abstract class LazyProperty extends ModelBase {
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;

    public LazyProperty(SecuregraphScript script, String propertyKey, String propertyName, Visibility propertyVisibility) {
        super(script);
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
    }

    @Override
    public String toString() {
        Property prop = getProperty();
        if (prop == null) {
            return null;
        }

        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);
        writer.println(getToStringHeaderLine());
        writer.println("  @|bold key:|@ " + getPropertyKey());
        writer.println("  @|bold name:|@ " + getPropertyName());
        writer.println("  @|bold visibility:|@ " + getPropertyVisibility());

        writer.println("  @|bold metadata:|@");
        for (Metadata.Entry m : prop.getMetadata().entrySet()) {
            writer.println("    " + m.getKey() + "[" + m.getVisibility() + "]: " + valueToString(m.getValue()));
        }

        writer.println("  @|bold value:|@");
        writer.println(valueToString(prop.getValue()));

        return out.toString();
    }

    @Override
    protected String valueToString(Object value) {
        if (value instanceof StreamingPropertyValue) {
            StreamingPropertyValue spv = (StreamingPropertyValue) value;
            if (spv.getValueType() == String.class) {
                try {
                    try (InputStream in = spv.getInputStream()) {
                        return IOUtils.toString(in);
                    }
                } catch (IOException e) {
                    throw new SecurityException("Could not get StreamingPropertyValue input stream", e);
                }
            }
        }

        return value.toString();
    }

    protected abstract String getToStringHeaderLine();

    protected abstract Property getProperty();

    public String getPropertyKey() {
        return propertyKey;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Visibility getPropertyVisibility() {
        return propertyVisibility;
    }
}
