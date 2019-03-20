package com.mks;

import java.io.Serializable;

public class Foo implements Serializable {
    int id;

   /* public Foo() {
    }

    public Foo(int id, String name, String tag) {
        this.id = id;
        this.name = name;
        this.tag = tag;
    }*/

    String name;

    @Override
    public String toString() {
        return "Foo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }

    String tag;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

}
