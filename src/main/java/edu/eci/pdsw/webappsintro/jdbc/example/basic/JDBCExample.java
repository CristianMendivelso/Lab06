/*
 * Copyright (C) 2015 hcadavid
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package edu.eci.pdsw.webappsintro.jdbc.example.basic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hcadavid
 */
public class JDBCExample {
    
    public static void main(String args[]){
        try {
            String url="jdbc:mysql://desarrollo.is.escuelaing.edu.co:3306/bdprueba";
            String driver="com.mysql.jdbc.Driver";
            String user="bdprueba";
            String pwd="bdprueba";
                        
            Class.forName(driver);
            Connection con=DriverManager.getConnection(url,user,pwd);
            con.setAutoCommit(false);
                 
            
            System.out.println("Valor total pedido 1:"+valorTotalPedido(con, 1));
            
            List<String> prodsPedido=nombresProductosPedido(con, 1);
            
            
            System.out.println("Productos del pedido 1:");
            System.out.println("-----------------------");
            for (String nomprod:prodsPedido){
                System.out.println(nomprod);
            }
            System.out.println("-----------------------");
            
            
            int suCodigoECI=2106088;
            registrarNuevoProducto(con, suCodigoECI, "SU NOMBRE", 99999999);            
            con.commit();
            
            cambiarNombreProducto(con, suCodigoECI, "EL NUEVO NOMBRE");
            con.commit();
            
            
            con.close();
                                   
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(JDBCExample.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
    }
    
    /**
     * Agregar un nuevo producto con los parámetros dados
     * @param con la conexión JDBC
     * @param codigo
     * @param nombre
     * @param precio
     * @throws SQLException 
     */
    public static void registrarNuevoProducto(Connection con, int codigo, String nombre,int precio) throws SQLException{
        //Crear preparedStatement
        PreparedStatement registrarProducto = null;
        String insertString = "insert into "+"bdprueba"+".ORD_PRODUCTOS"+"(codigo,nombre,precio)"+
                                   "values ( ? , ? , ? )"; 
        
        try{
        con.setAutoCommit(false);

            registrarProducto= con.prepareStatement(insertString);
            //Asignar parámetros
            registrarProducto.setInt(1,codigo);
            registrarProducto.setString(2,nombre);
            registrarProducto.setInt(3,precio);
            //usar 'execute'
            registrarProducto.execute();


            con.commit();
        } catch (SQLException e ) {
        //JDBCTutorialUtilities.printSQLException(e);
        if (con != null) {
            try {
                System.err.print("Transaction is being rolled back");
                con.rollback();
            } catch(SQLException excep) {
                //JDBCTutorialUtilities.printSQLException(excep);
            }
        }
    } finally {
        if (registrarProducto != null) {
            registrarProducto.close();
        }
        if (registrarProducto != null) {
            registrarProducto.close();
        }
        con.setAutoCommit(true);
    }
        con.close();
    }
    
    /**
     * Consultar los nombres de los productos asociados a un pedido
     * @param con la conexión JDBC
     * @param codigoPedido el código del pedido
     * @return 
     */
    public static List<String> nombresProductosPedido(Connection con, int codigoPedido) throws SQLException{
        List<String> np=new LinkedList<>();
        
        //Crear prepared statement
        PreparedStatement consultaProducto = null;
        String consulta= "select "+ "bdprueba.ord_productos.nombre"+  " from "+"bdprueba.ord_prodcutos"
                +"where "+"bdprueba.ord_productos.codigo"+"="+"bdprueba.ord_detalles_pedido.producto_fk"+"and "
                +"bdprueba.ord_detalles_pedido.producto_fk"+"="+"?";
        try{
            con.setAutoCommit(false);
            consultaProducto= con.prepareStatement(consulta);
            //asignar parámetros
            consultaProducto.setInt(1,codigoPedido);
            //usar executeQuery
            ResultSet rs= consultaProducto.executeQuery(consulta);
            con.commit();
            //Sacar resultados del ResultSet
            for(int i=0;i<rs.getFetchSize();i++){
                np.add(rs.getString(i));
            }

            //Llenar la lista y retornarla
        }
        catch (SQLException e ) {
        //JDBCTutorialUtilities.printSQLException(e);
        if (con != null) {
            try {
                System.err.print("Transaction is being rolled back");
                con.rollback();
            } catch(SQLException excep) {
                //JDBCTutorialUtilities.printSQLException(excep);
            }
        }
    } finally {
        if (consultaProducto != null) {
            consultaProducto.close();
        }
        if (consultaProducto != null) {
            consultaProducto.close();
        }
        con.setAutoCommit(true);
    }
        con.close();
        return np;
    }

    
    /**
     * Calcular el costo total de un pedido
     * @param con
     * @param codigoPedido código del pedido cuyo total se calculará
     * @return el costo total del pedido (suma de: cantidades*precios)
     */
    public static int valorTotalPedido(Connection con, int codigoPedido) throws SQLException{
        
        //Crear prepared statement
        PreparedStatement  valorTotal = null;
        String valor="SELECT "+"SUM(ORD_DETALLES_PEDIDO.CANTIDAD*ORD_PRODUCTOS.PRECIO) "
                + "FROM "+"ORD_PEDIDOS, ORD_DETALLES_PEDIDO, ORD_PRODUCTOS "
                + "WHERE "+"ORD_PEDIDOS.CODIGO"+"="+"ORD_DETALLES_PEDIDO.PEDIDO_FK "
                + "AND "+"ORD_DETALLES_PEDIDO.PRODUCTO_FK "+"="+" ORD_PRODUCTOS.CODIGO"
                + "AND "+"ORD_PEDIDOS.CODIGO "+"="+"?";
        int ans=0;
        
        try{
            con.setAutoCommit(false);
            valorTotal=con.prepareStatement(valor);
            //asignar parámetros
            valorTotal.setInt(1, codigoPedido);
            //usar executeQuery
            ResultSet rs = valorTotal.executeQuery();
            con.commit();
            //Sacar resultado del ResultSet
            ans=rs.getInt(1);
        }
        catch (SQLException e ) {
        //JDBCTutorialUtilities.printSQLException(e);
        if (con != null) {
            try {
                System.err.print("Transaction is being rolled back");
                con.rollback();
            } catch(SQLException excep) {
                //JDBCTutorialUtilities.printSQLException(excep);
            }
        }
    } finally {
        if (valorTotal != null) {
            valorTotal.close();
        }
        if (valorTotal != null) {
            valorTotal.close();
        }
        con.setAutoCommit(true);
    }
        con.close();
        return ans;
    }
    

    /**
     * Cambiar el nombre de un producto
     * @param con
     * @param codigoProducto codigo del producto cuyo nombre se cambiará
     * @param nuevoNombre el nuevo nombre a ser asignado
     */
    public static void cambiarNombreProducto(Connection con, int codigoProducto, String nuevoNombre) throws SQLException{

        //Crear prepared statement
        PreparedStatement  updateProducto = null;
        PreparedStatement  consultaProducto = null;
        String updateString = "update " + "dbprueba" + ".ORD_PRODUCTOS " +
                "set nombre = ? where codigo = ?";
        try{
            con.setAutoCommit(false);
            updateProducto = con.prepareStatement(updateString);
            //asignar parámetros
            updateProducto.setString(1,nuevoNombre);
            updateProducto.setInt(2,codigoProducto);
            //usar executeUpdate
            updateProducto.executeUpdate();
            con.commit();
            //verificar que se haya actualizado exactamente un registro
            String consulta = "select nombre from dbprueba.ORD_PRODUCTOS Where codigo="+"?";
            consultaProducto= con.prepareStatement(consulta);
            consultaProducto.executeQuery();
            con.commit();
        }
        catch (SQLException e ) {
        //JDBCTutorialUtilities.printSQLException(e);
        if (con != null) {
            try {
                System.err.print("Transaction is being rolled back");
                con.rollback();
            } catch(SQLException excep) {
                //JDBCTutorialUtilities.printSQLException(excep);
            }
        }
    } finally {
        if (updateProducto != null) {
            updateProducto.close();
        }
        if (updateProducto != null) {
            updateProducto.close();
        }
        con.setAutoCommit(true);
    }
        try{
            con.setAutoCommit(false);
            //verificar que se haya actualizado exactamente un registro
            String consulta = "select nombre from dbprueba.ORD_PRODUCTOS Where codigo="+"?";
            consultaProducto= con.prepareStatement(consulta);
            consultaProducto.executeQuery();
            con.commit();
        }
        catch (SQLException e ) {
        //JDBCTutorialUtilities.printSQLException(e);
        if (con != null) {
            try {
                System.err.print("Transaction is being rolled back");
                con.rollback();
            } catch(SQLException excep) {
                //JDBCTutorialUtilities.printSQLException(excep);
            }
        }
    } finally {
        if (updateProducto != null) {
            updateProducto.close();
        }
        if (updateProducto != null) {
            updateProducto.close();
        }
        con.setAutoCommit(true);
    }
        con.close();
        
        
        
    }
    
    
    
}
