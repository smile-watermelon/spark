package com.guagua.rdd.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.chrono.IsoEra;

/**
 * @author guagua
 * @date 2023/3/1 11:02
 * @describe
 */
public class ObjectSerial {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        javaSerial();
        javaDeSerial();
        kryoSerial();
        kryoDeSerial();

    }

    private static void kryoDeSerial() throws IOException {
        Kryo kryo = new Kryo();
        Input input = new Input(Files.newInputStream(Paths.get("data/user-kryo.txt")));
        User user = kryo.readObject(input, User.class);
        System.out.println(user.getAge() + " " + user.getUsername());
        input.close();
    }

    private static void kryoSerial() throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(User.class);

        User user = new User();
        user.setUsername("guagua");
        user.setAge(10);

        Output output = new Output(Files.newOutputStream(Paths.get("data/user-kryo.txt")));
        kryo.writeObject(output, user);
        output.close();
    }

    private static void javaDeSerial() throws IOException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(Files.newInputStream(Paths.get("data/user.txt")));
        User user = (User) in.readObject();
        System.out.println(user.getAge() + " " + user.getUsername());
    }

    private static void javaSerial() throws IOException {
        User user = new User();
        user.setAge(10);
        user.setUsername("guagua");
        ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(Paths.get("data/user.txt")));
        out.writeObject(user);
        out.flush();

        out.close();
    }

    static class User implements Serializable {
        private String username;

        private int age;

        public User() {
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
