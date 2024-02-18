package ua.kiev.prog.case2;

import ua.kiev.prog.shared.Client;
import ua.kiev.prog.shared.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class Main {
    public static void main(String[] args) throws SQLException {
        try (Connection conn = ConnectionFactory.getConnection())
        {
            // remove this
            try {
                try (Statement st = conn.createStatement()) {
                    st.execute("DROP TABLE IF EXISTS Clients");
                    //st.execute("CREATE TABLE Clients (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20) NOT NULL, age INT)");
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }

            ClientDAOImpl2 dao = new ClientDAOImpl2(conn, "Clients");
            dao.createTable(Client.class);

            Client c = new Client("test1", 18, "bi");
            dao.add(c);
            int id = c.getId();
            System.out.println("id = " + id);

            c = new Client("test2", 22, "man");
            dao.add(c);
            System.out.println("id = " + c.getId());

            List<Client> list = dao.getAll(Client.class);
            for (Client cli : list)
                System.out.println(cli);

            list.get(0).setAge(55);
            dao.update(list.get(0));

            list = dao.getAll(Client.class, "name", "age", "gender");
            for (Client cli : list)
                System.out.println(cli);
            System.out.println("\n");
            list = dao.getAll(Client.class, "name", "age", "pipo");
            for (Client cli : list)
                System.out.println(cli);
            System.out.println("\n");
            list = dao.getAll(Client.class, "name", "age");
            for (Client cli : list)
                System.out.println(cli);
            System.out.println("\n");
            list = dao.getAll(Client.class, "age");
            for (Client cli : list)
                System.out.println(cli);
            System.out.println("\n");
            dao.delete(list.get(0));
        }
    }
}
