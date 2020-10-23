package com.ppe.db.trial;


import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface GSSortKey {
    String[] index();
}



