from flask import Flask, request, jsonify
import psycopg2
from flask_cors import CORS


app = Flask(__name__)

# Connect to the database
conn = psycopg2.connect(
    host="localhost",
    database="proyecto",
    user="postgres",
)

CORS(app, origins=['http://localhost:8080'])


@app.route('/bd-query1')
def query1():
    cur = conn.cursor()
    set_search_path = "set search_path to mil;"
    query = "select p.numero_documento, p.nombre, p.apellido, e.sueldo, a.casos_ganados, a.casos_perdidos from ((select ap.numero_documento_abogado, ap.tipo_documento_abogado        from abogadoparticipa ap                 join caso c on ap.caso_codigo = c.codigo       where c.estado = 'EnProceso'         and ap.tipo_documento_abogado = 'DNI'       intersect       select s.numero_documento_abogado, s.tipo_documento_abogado       from secretarioasiste s                join abogado ab on s.numero_documento_abogado = ab.numero_documento and                                   s.tipo_documento_abogado = ab.tipo_documento) query    join persona p on query.tipo_documento_abogado = p.tipo_documento and                      query.numero_documento_abogado = p.numero_documento    natural join empleado e natural join abogado a)    where a.casos_ganados < 0.33*(a.casos_perdidos + a.casos_ganados)    and e.tiempo_parcial = false and e.sueldo>(select avg(sueldo)from empleado natural join abogado);"    
    cur.execute(set_search_path)
    cur.execute(query)
    data = cur.fetchall()
    
    return jsonify(data)

@app.route('/bd-query2')
def query2():
    cur = conn.cursor()
    set_search_path = "set search_path to cienmil;"
    cur.execute(set_search_path)
    query = "SELECT AP.tipo_documento_abogado, AP.numero_documento_abogado, count((AP.tipo_documento_abogado, AP.numero_documento_abogado)) AS cantidad_casos FROM AbogadoParticipa AP         JOIN PersonaRepresenta PR ON AP.caso_codigo = PR.caso_codigo WHERE PR.ruc = '18164809220' AND (AP.tipo_documento_abogado, AP.numero_documento_abogado) IN (    SELECT tipo_documento, numero_documento    FROM Persona P NATURAL JOIN Empleado E NATURAL JOIN Abogado A    WHERE P.sexo = 'F' AND E.fecha_inicio > now() - INTERVAL '1 year'    ORDER BY A.casos_ganados DESC    LIMIT (SELECT round(0.2*count((A2.tipo_documento, A2.numero_documento))) FROM Abogado A2)    )GROUP BY AP.tipo_documento_abogado, AP.numero_documento_abogado;"
    cur.execute(query)
    data = cur.fetchall()
    
    return jsonify(data)

@app.route('/bd-query3')
def query3():
    cur = conn.cursor()
    set_search_path = "set search_path to mil;"
    cur.execute(set_search_path)
    query = "SELECT E.tipo_documento, E.numero_documento, A.casos_ganados FROM Empleado E NATURAL JOIN Abogado A WHERE A.casos_ganados >= (    SELECT percentile_cont(0.80) WITHIN GROUP ( ORDER BY A2.casos_ganados )    FROM Abogado A2    ) AND NOT E.tiempo_parcial AND EXISTS(    SELECT 1    FROM Departamento D JOIN AbogadoTrabaja AT    ON D.nombre = AT.nombre_departamento    AND (A.tipo_documento, A.numero_documento) = (AT.tipo_documento_abogado, AT.numero_documento_abogado)    WHERE D.fecha_creacion <= now() - INTERVAL '2 years')AND now() - E.fecha_inicio >= INTERVAL '1 year'"
    cur.execute(query)
    data = cur.fetchall()
    
    return jsonify(data)

@app.route('/sql', methods=['POST'])
def sql():
    cur = conn.cursor()
    set_search_path = "set search_path to mil;"
    cur.execute(set_search_path)
    query = request.get_json()['query']
    print(query)
    # expresión regular para hacer que solo se puedan hacer consultas de tipo SELECT
    if query[0:6].upper() != 'SELECT':
        cur.close()
        return jsonify({'success': False, 'info': 'Solo se permiten consultas de tipo SELECT'})
    # expresion regular para evitar que se puedan hacer consultas de tipo DROP, DELETE, UPDATE, INSERT
    if 'DROP' in query.upper() or 'DELETE' in query.upper() or 'UPDATE' in query.upper() or 'INSERT' in query.upper():
        cur.close()
        return jsonify({'success': False, 'info': 'No se permiten consultas de tipo DROP, DELETE, UPDATE, INSERT'})
    try:
        cur.execute(query)
    except Exception as e:
        cur.close()
        return jsonify({'success': False, 'info': 'Error en la consulta'})
    print(query)
    data = cur.fetchall()
    return jsonify({'success':True, 'info':data})

@app.route('/personas',methods=['GET'])
def get_abogados():
    cur = conn.cursor()
    set_search_path = "set search_path to mil;"
    cur.execute(set_search_path)
    query = "SELECT * FROM persona Limit 10"
    cur.execute(query)
    data = cur.fetchall()
    return jsonify(data)



@app.route('/casos')
def get_casos():
    cur = conn.cursor()
    set_search_path = "set search_path to mil;"
    cur.execute(set_search_path)
    cur.execute('SELECT * FROM casos limit 10')
    data = cur.fetchall()
    return jsonify(data)

@app.route('/secretarios')
def get_secretarios():
    cur = conn.cursor()
    set_search_path = "set search_path to mil;"
    cur.execute(set_search_path)
    cur.execute('SELECT * FROM secretarios limit 10')
    data = cur.fetchall()
    return jsonify({'success': True, 'secretarios': data})




if __name__ == '__main__':
    app.run(debug=True, port=5004)
