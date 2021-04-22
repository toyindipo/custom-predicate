package com.upperlink.billerservice.repository.predicate;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class CustomPredicate {
    private String field;
    private Comparable value;
    private List<Comparable> otherValues = new ArrayList<>();
    private Operation operation;
    private boolean negate;

    /**
     *
     * @param field
     * @param value
     */
    public CustomPredicate(String field, Comparable value) {
        this(field, value, Operation.EQUALS);
    }

    public CustomPredicate(String field, Comparable value, Operation operation) {
        this(field, value, operation, false);
    }

    public CustomPredicate(String field, Comparable value, Operation operation, boolean negate) {
        this.field = field;
        this.value = value;
        this.operation = operation;
        this.negate = negate;
    }

    public CustomPredicate addOtherValue(Comparable value) {
        if (otherValues == null) otherValues = new ArrayList<>();
        otherValues.add(value);
        return this;
    }
}
