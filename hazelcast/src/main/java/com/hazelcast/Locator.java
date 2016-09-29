package com.hazelcast;

import com.hazelcast.client.ClientProtocol;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ConfigurationBuilder;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

public class Locator {

    public static void main(String[] args) throws ClassNotFoundException {
        Reflections reflections = new Reflections(
                new ConfigurationBuilder().forPackages("com.hazelcast")
                        .addScanners(new SubTypesScanner()).build());

        Collection allDSClasses = reflections.getSubTypesOf(DataSerializable.class);
        Collection allIDSClasses = reflections.getSubTypesOf(IdentifiedDataSerializable.class);

        boolean itemsRemoved = allDSClasses.removeAll(allIDSClasses);

        if (!itemsRemoved) {
            System.out.println("Hooray! All operation classes implement IdentifiedDataSerializable!");
        } else {
            Set<String> classNames = new TreeSet<String>();
            for (Object o : allDSClasses) {
                classNames.add(((Class) (o)).getName());
            }
            int i = 0;
            for (String s : classNames) {
                Class clazz = Class.forName(s);
                if(clazz.isInterface()) {
                    continue;
                }
                boolean o = clazz.isAnnotationPresent(ClientProtocol.class);
                System.out.println(i++ + "\t" + (o ? "[+]" : "[ ]") + "\t" + s);
            }
        }
    }
}
