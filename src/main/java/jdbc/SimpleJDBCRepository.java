package jdbc;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SimpleJDBCRepository {

    private CustomDataSource dataSource = CustomDataSource.getInstance();
    private PreparedStatement ps = null;
    private Statement st = null;

    private static final String CREATE_USER_SQL = """
            INSERT INTO myusers(firstname, lastname, age)
            VALUES (?, ?, ?);
            """;
    private static final String UPDATE_USER_SQL = """
            UPDATE myusers
            SET firstname = ?, lastname = ?, age = ?
            WHERE id = ?
            """;
    private static final String DELETE_USER = """
            DELETE FROM myusers
            WHERE id = ?
            """;
    private static final String FIND_USER_BY_ID_SQL = """
            SELECT * FROM myusers
            WHERE id = ?
            """;
    private static final String FIND_USER_BY_NAME_SQL = """
            SELECT * FROM myusers
            WHERE firstname LIKE CONCAT(?, %)
            """;
    private static final String FIND_ALL_USER_SQL = """
            SELECT * FROM myusers
            """;

    public Long createUser(User user) {
        Long id = null;
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(CREATE_USER_SQL, Statement.RETURN_GENERATED_KEYS)) {
            statement.setObject(1, user.getFirstName());
            statement.setObject(2, user.getLastName());
            statement.setObject(3, user.getAge());
            statement.execute();
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()){
                id = resultSet.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }

    public User findUserById(Long userId) {
        try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(FIND_USER_BY_ID_SQL)) {
            statement.setLong(1, userId);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()){
                return map(resultSet);
            } else return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public User findUserByName(String userName) {
        try(Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(FIND_USER_BY_NAME_SQL)){
            statement.setString(1,userName);
            ResultSet rs = statement.executeQuery();
            if (rs.next()){
                return map(rs);
            } else return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<User> findAllUser() {
        List<User> userList = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(FIND_ALL_USER_SQL)){
            ResultSet rs = statement.executeQuery();
            while (rs.next()){
                userList.add(map(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return userList;
    }

    public User updateUser(User user) {
        try(Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(UPDATE_USER_SQL)){
            statement.setString(1, user.getFirstName());
            statement.setString(2, user.getLastName());
            statement.setInt(3, user.getAge());
            statement.setLong(4, user.getId());

            if (statement.executeUpdate() != 0){
                return findUserById(user.getId());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public void deleteUser(Long userId) {
        try(Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(DELETE_USER)) {
            statement.setLong(1,userId);
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private User map(ResultSet rs) throws SQLException {
        return User.builder()
                .id(rs.getLong(1))
                .firstName(rs.getString(2))
                .lastName(rs.getString(3))
                .age(rs.getInt(4))
                .build();
    }
}
