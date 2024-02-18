package ua.kiev.prog.case2;

import ua.kiev.prog.shared.Id;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractDAO<T> {
    private final Connection conn;
    private final String table;
    private Map<Class, String> supportedClassTypes;

    public AbstractDAO(Connection conn, String table)  {
        this.conn = conn;
        this.table = table;
        setClassTypes();
    }

    public void createTable(Class<T> cls) {
        Field[] fields = cls.getDeclaredFields();
        Field id = getPrimaryKeyField(null, fields);

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ")
                .append(table)
                .append("(");

        sql.append(id.getName())
                .append(" ")
                .append(" INT AUTO_INCREMENT PRIMARY KEY,");

        for (Field f : fields) {
            if (f != id) {
                f.setAccessible(true);

                sql.append(f.getName()).append(" ");
                String type = supportedClassTypes.get(f.getType());
                if (type!=null){
                    sql.append(type);
                }else throw new RuntimeException("Wrong type");
//                if (f.getType() == int.class) {
//                    sql.append("INT,");
//                } else if (f.getType() == String.class) {
//                    sql.append("VARCHAR(100),");
//                } else if (f.getType() == String.class) {
//                    sql.append("VARCHAR(50),");
//                } else
//                    throw new RuntimeException("Wrong type");
            }
        }

        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");

        try {
            try (Statement st = conn.createStatement()) {
                st.execute(sql.toString());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void add(T t) {
        try {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = getPrimaryKeyField(t, fields);

            StringBuilder names = new StringBuilder();
            StringBuilder values = new StringBuilder();

            // insert into t (name,age) values("..",..)

            for (Field f : fields) {
                if (f != id) {
                    f.setAccessible(true);
                    names.append(f.getName()).append(',');
                    if (f.get(t).getClass().equals(Boolean.class)) {
                        values.append('"').append(f.get(t).equals(false) ? 0 : 1).append("\",");
                    } else {
                        values.append('"').append(f.get(t)).append("\",");
                    }
                }
            }

            names.deleteCharAt(names.length() - 1); // last ','
            values.deleteCharAt(values.length() - 1);

            String sql = "INSERT INTO " + table + "(" + names.toString() +
                    ") VALUES(" + values.toString() + ")";

            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }

            // TODO: get ID
            // SELECT - X
            sql = "SELECT count(*) AS total FROM " + table;
            try (Statement st = conn.createStatement()) {
                ResultSet rs = st.executeQuery(sql);
                rs.next();
                id.setAccessible(true);
                id.set(t, rs.getInt("total"));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void update(T t) {
        try {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = getPrimaryKeyField(t, fields);

            StringBuilder sb = new StringBuilder();

            for (Field f : fields) {
                if (f != id) {
                    f.setAccessible(true);

                    sb.append(f.getName())
                            .append(" = ")
                            .append('"');
                    if (f.get(t).getClass().equals(Boolean.class)) {
                        sb.append(f.get(t).equals(false) ? 0 : 1);
                    } else {
                        sb.append(f.get(t));
                    }
                    sb.append('"')
                            .append(',');
                }
            }

            sb.deleteCharAt(sb.length() - 1);

            // update t set name = "aaaa", age = "22" where id = 5
            String sql = "UPDATE " + table + " SET " + sb.toString() + " WHERE " +
                    id.getName() + " = \"" + id.get(t) + "\"";

            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void delete(T t) {
        try {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = getPrimaryKeyField(t, fields);

            // delete from t where id = x
            String sql = "DELETE FROM " + table + " WHERE " + id.getName() +
                    " = \"" + id.get(t) + "\"";

            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<T> getAll(Class<T> cls, String... ars) {
        List<T> res = new ArrayList<>();

        try {
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT * FROM " + table)) {
                    ResultSetMetaData md = rs.getMetaData();
                    if (ars.length == 0) ars = getAllColumNames(md);
                    while (rs.next()) {
                        T t = cls.getDeclaredConstructor().newInstance(); //!!!

                        for (String argument : ars) {
                            try {
                                Field field = cls.getDeclaredField(argument);
                                field.setAccessible(true);
                                field.set(t, rs.getObject(argument));
                            } catch (NoSuchFieldException e) {
                                System.out.println("There is not field named : " + argument);
                            }
                        }

                        res.add(t);
                    }
                }
            }

            return res;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    private String[] getAllColumNames(ResultSetMetaData md) throws SQLException {
        String[] args = new String[md.getColumnCount()];
        for (int i = 1; i <= md.getColumnCount(); i++) {
            args[i - 1] = md.getColumnName(i);
        }
        return args;
    }
    private Field getPrimaryKeyField(T t, Field[] fields) {
        Field result = null;

        for (Field f : fields) {
            if (f.isAnnotationPresent(Id.class)) {
                result = f;
                result.setAccessible(true);
                break;
            }
        }

        if (result == null)
            throw new RuntimeException("No Id field found");

        return result;
    }
    private void setClassTypes() {
        supportedClassTypes = new HashMap<>();
        supportedClassTypes.put(int.class, "INT,");
        supportedClassTypes.put(String.class, "VARCHAR(100),");
        supportedClassTypes.put(String.class, "VARCHAR(100),");
    }
}
