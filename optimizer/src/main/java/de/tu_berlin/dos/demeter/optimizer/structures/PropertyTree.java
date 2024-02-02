package de.tu_berlin.dos.demeter.optimizer.structures;

import de.tu_berlin.dos.demeter.optimizer.utils.CheckedConsumer;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PropertyTree implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public static PropertyTree of(String key) {

        String[] keys = key.split("\\.");
        PropertyTree previous = null;
        for (String s : keys) {

            PropertyTree entry = new PropertyTree(s);
            if (previous != null) previous.addChild(entry);
            previous = entry;
        }
        return previous;
    }

    public static PropertyTree of(String key, String value) {

        return PropertyTree.of(key).setValue(value);
    }

    public static void merge(PropertyTree a, PropertyTree b) {

        PropertyTree bClone = (PropertyTree) SerializationUtils.clone(b);

        if (!a.equals(bClone)) {

            throw new IllegalStateException(String.format("Merge Error: %s and %s do not match", a.key, b.key));
        }
        if (a.children.size() == 0) {

            for (PropertyTree bChild : bClone.children) {

                a.addChild(bChild);
            }
        }
        else {

            boolean match = false;
            Iterator<PropertyTree> bIter = b.children.iterator();
            while (bIter.hasNext()) {

                PropertyTree bChild = bIter.next();
                Iterator<PropertyTree> aIter = a.children.iterator();
                while (aIter.hasNext()) {

                    PropertyTree aChild = aIter.next();
                    if (bChild.equals(aChild)) {
                        merge(aChild, bChild);
                        match = true;
                    }
                }
                if (!match) a.addChild(bChild);
            }
        }
    }

    public PropertyTree parent;
    public final String key;
    public String value;
    public final List<PropertyTree> children;

    private PropertyTree(String key) {

        this(null, key, null);
    }

    private PropertyTree(PropertyTree parent, String key, String value) {

        this.parent = parent;
        if (this.parent != null) this.parent.children.add(this);
        this.key = key;
        this.value = value;
        this.children = new ArrayList<>();
    }

    public void addChild(PropertyTree child) {

        child.parent = this;
        this.children.add(child);
    }

    public void removeChild(String key) {

        for (int i = 0; i < this.children.size(); i++) {

            String k2 = this.children.get(i).key;
            if (k2.equals(key)) {

                this.children.remove(i);
            }
        }
    }

    public PropertyTree getRoot() {

        if (this.parent == null) return this;
        else return this.parent.getRoot();
    }

    public PropertyTree setValue(String value) {

        this.value = value;
        return this;
    }

    public List<String> childrenValues() throws Exception {

        List<String> values = new ArrayList<>();
        this.forEach(child -> {

            if (child.value != null) values.add(child.value);
        });
        return values;
    }

    public PropertyTree find(String key) {

        if (this.key.equals(key)) return this;
        for (PropertyTree child : this.children) {

            PropertyTree found = child.find(key);
            if (found != null) return found;
        }
        return null;
    }

    public boolean exists(String key) {

        if (this.key.equals(key)) return true;
        for (PropertyTree child : this.children) {

            boolean found = child.exists(key);
            if (found) return true;
        }
        return false;
    }

    public void forEach(CheckedConsumer<PropertyTree> behaviour) throws Exception {

        for (PropertyTree current : this.children) {

            behaviour.accept(current);
        }
    }

    @Override
    public boolean equals(Object o) {

        if (!(o instanceof PropertyTree)) return false;
        return this.key.equals(((PropertyTree) o).key);
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        if (this.children.isEmpty()) return String.format("{ %s : %s }", this.key, this.value);
        else if (this.value == null) {

            sb.append(String.format("{ %s: ", this.key));
            for (int i = 0; i < this.children.size(); i++) {

                PropertyTree child = this.children.get(i);
                sb.append(String.format("%s", child.toString()));
                if (i < this.children.size() - 1) sb.append(",");
            }
        }
        sb.append(" }");
        return sb.toString();
    }
}
