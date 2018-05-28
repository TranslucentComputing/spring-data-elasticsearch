package org.springframework.data.elasticsearch.core.query;

/**
 * User: patryk
 * Date: 2015-11-26
 * Time: 10:07 AM
 */
public final class NestedField extends SimpleField
{
    boolean nested;

    public NestedField(String name, boolean nested) {
        super(name);
        this.nested = nested;
    }

    public boolean isNested() {
        return nested;
    }
}
