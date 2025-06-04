from flask import Flask, request, jsonify, copy_current_request_context
from flask_cors import CORS
import mysql.connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import atexit
from sqlalchemy import text


app = Flask(__name__)
CORS(app)

# Konfigurasi Email & Database
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
GMAIL_USER = 'ahmadalfajri173@gmail.com'
GMAIL_PASS = 'pkxa vkcd fins awpq'

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'tracer'
}

# 🔁 Scheduler Setup
scheduler = BackgroundScheduler()
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

# Fungsi Kirim Email
def send_email(to_email, subject, html_body):
    msg = MIMEMultipart()
    msg['From'] = GMAIL_USER
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(html_body, 'html'))

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(GMAIL_USER, GMAIL_PASS)
    server.send_message(msg)
    server.quit()

def generate_id_email_blast(cursor):
    cursor.execute("SELECT idEmailBlast FROM emailblast WHERE idEmailBlast LIKE 'EM%' ORDER BY idEmailBlast DESC LIMIT 1")
    last_id = cursor.fetchone()
    number = int(last_id['idEmailBlast'][2:]) + 1 if last_id else 1
    return f"EM{str(number).zfill(3)}"

def generate_id_detail_email(cursor):
    cursor.execute("SELECT idDetailEmail FROM detailemailblast WHERE idDetailEmail LIKE 'DEM%' ORDER BY idDetailEmail DESC LIMIT 1")
    last_id = cursor.fetchone()
    number = int(last_id['idDetailEmail'][3:]) + 1 if last_id else 1
    return f"DEM{str(number).zfill(3)}"

@app.route('/api/tahun-lulus', methods=['GET'])
def get_tahun_lulus():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT cohort FROM lulusan WHERE cohort IS NOT NULL ORDER BY cohort DESC")
    result = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify(result)


@app.route('/api/email-blast', methods=['POST'])
def blast_email():
    data = request.json
    print("DATA DITERIMA:", data)

    # ✅ Validasi namaEmailBlast: jika sudah ada, hentikan proses
    try: 
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM emailblast WHERE namaEmailBlast = %s"
        cursor.execute(query, (data['namaEmailBlast'],))
        (count,) = cursor.fetchone()
        if count > 0:
            print(f"❌ Nama email blast '{data['namaEmailBlast']}' sudah digunakan.")
            return jsonify({"error": f"Nama email blast '{data['namaEmailBlast']}' sudah ada."}), 400
    except Exception as e:
        print("🔥 ERROR saat validasi namaEmailBlast:", str(e))
        return jsonify({"error": f"Kesalahan saat validasi namaEmailBlast: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # ✅ Lanjutkan proses hanya jika namaEmailBlast belum ada
    try:
        scheduled_time = datetime.strptime(data['periodeBlastE'], "%Y-%m-%d %H:%M:%S")
        now = datetime.now()

        if scheduled_time > now:
            print("⏳ Menjadwalkan email pada:", scheduled_time)

            @copy_current_request_context
            def scheduled_job():
                print("🚀 Menjalankan job terjadwal pada:", datetime.now())
                run_blast_job(data)

            scheduler.add_job(
                scheduled_job,
                trigger='date',
                run_date=scheduled_time,
                id=f"job_{scheduled_time.strftime('%Y%m%d%H%M%S')}_{data['namaEmailBlast']}",
                replace_existing=True
            )
            return jsonify({"message": f"Email dijadwalkan pada {scheduled_time}"}), 200
        else:
            print("📤 Mengirim langsung karena waktu terlewat.")
            return run_blast_job(data)
    except Exception as e:
        print("🔥 ERROR saat proses pengiriman:", str(e))
        return jsonify({"error": f"Format periodeBlastE salah: {str(e)}"}), 400

    
@app.route('/api/email-blast/<idEmailBlast>', methods=['GET'])
def get_email_blast_by_id(idEmailBlast):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM emailblast WHERE idEmailBlast = %s", (idEmailBlast,))
        result = cursor.fetchone()

        if not result:
            return jsonify({"error": "Data tidak ditemukan"}), 404

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close() 

@app.route('/api/email-pesan/<idEmailBlast>', methods=['GET'])
def get_email_blast_by_id_pesan(idEmailBlast):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM emailblast WHERE idEmailBlast = %s", (idEmailBlast,))
        result = cursor.fetchone()

        if not result:
            return jsonify({"error": "Data tidak ditemukan"}), 404

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route('/api/email-blast-update', methods=['PUT'])
def blast_email_update_only():
    data = request.json
    print("DATA DITERIMA UNTUK UPDATE:", data)

    if 'idEmailBlast' not in data:
        return jsonify({"error": "idEmailBlast wajib diisi untuk update"}), 400

    try:
        return update(data)
    except Exception as e:
        return jsonify({"error": f"Gagal update: {str(e)}"}), 500
    
    
@app.route('/api/custom-pesan', methods=['PUT'])
def blast_email_pesan():
    data = request.json
    print("DATA DITERIMA:", data)

    if 'idEmailBlast' in data:
        print("🛠 Mode UPDATE EmailBlast:", data['idEmailBlast'])

    try:
        scheduled_time = datetime.strptime(data['periodeBlastE'], "%Y-%m-%d %H:%M:%S")
        now = datetime.now()

        if scheduled_time > now:
            print("⏳ Menjadwalkan email pada:", scheduled_time)

            @copy_current_request_context
            def scheduled_job():
                print("🚀 Menjalankan job terjadwal pada:", datetime.now())
                run_blast_job_custompesan(data)

            scheduler.add_job(
                scheduled_job,
                trigger='date',
                run_date=scheduled_time,
                id=f"job_{scheduled_time.strftime('%Y%m%d%H%M%S')}_{data['namaEmailBlast']}",
                replace_existing=True
            )
            return jsonify({"message": f"Email dijadwalkan pada {scheduled_time}"}), 200
        else:
            print("📤 Mengirim langsung karena waktu terlewat.")
            return run_blast_job_custompesan(data)
    except Exception as e:
        return jsonify({"error": f"Format periodeBlastE salah: {str(e)}"}), 400

def run_blast_job_custompesan(data):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        tahun_lulus = int(data['targetTahunLulusan']) - 1
        subject = data['subjek']
        nama_email_blast = data['namaEmailBlast']
        isi_email = data['isiEmail']
        path_file = data['pathFile']
        periode = data['periodeBlastE']
        status = 'S'

        cursor.execute("SELECT nama, email FROM lulusan WHERE tahunLulus = %s AND email IS NOT NULL AND email != ''", (tahun_lulus,))
        users = cursor.fetchall()

        id_email_blast = data.get('idEmailBlast')

        if id_email_blast:
            print(f"✏️ Update EmailBlast dengan ID: {id_email_blast}")

            # 1. Set idDetailEmail ke NULL terlebih dahulu di lulusan
            cursor.execute("""
                UPDATE lulusan
                SET idDetailEmail = NULL
                WHERE idDetailEmail IN (
                    SELECT idDetailEmail FROM detailemailblast WHERE idEmailBlast = %s
                )
            """, (id_email_blast,))

            # 2. Hapus detail email blast lama
            cursor.execute("DELETE FROM detailemailblast WHERE idEmailBlast = %s", (id_email_blast,))

            # 3. Update master email blast
            cursor.execute("""
                UPDATE emailblast SET 
                    namaEmailBlast=%s, 
                    targetTahunLulusan=%s, statusEmailBlast=%s, subjek=%s, isiEmail=%s, pathFile=%s, periodeBlastE=%s 
                WHERE idEmailBlast=%s
            """, (
                nama_email_blast, tahun_lulus,
                status, subject, isi_email, path_file, periode, id_email_blast
            ))
        else:
            id_email_blast = generate_id_email_blast(cursor)
            cursor.execute("""
                INSERT INTO emailblast 
                (idEmailBlast, namaEmailBlast,  targetTahunLulusan, 
                statusEmailBlast, subjek, isiEmail, pathFile, periodeBlastE) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                id_email_blast, nama_email_blast, tahun_lulus,
                status, subject, isi_email, path_file, periode
            ))

        for user in users:
            email = user['email']
            name = user['nama']
            body = isi_email.replace('{nama}', name)

            try:
                send_email(email, subject, body)
            except Exception as e:
                print(f"❌ Gagal kirim ke {email}: {e}")
                status = 'G'

            id_detail = generate_id_detail_email(cursor)
            cursor.execute("""
                INSERT INTO detailemailblast (idDetailEmail, idEmailBlast, statusPengirimanE)
                VALUES (%s, %s, %s)
            """, (id_detail, id_email_blast, status))

            cursor.execute("""
                UPDATE lulusan
                SET idDetailEmail = %s
                WHERE nama = %s AND email = %s AND tahunLulus = %s
            """, (id_detail, user['nama'], user['email'], tahun_lulus))

        conn.commit()
        print("✅ Pesan tersimpan !")
        return jsonify({"message": "Pesan tersimpan !"})
    except Exception as e:
        print("🔥 ERROR:", str(e))
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update(data):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        subject = data['subjek']
        nama_email_blast = data['namaEmailBlast']
        isi_email = data['isiEmail']
        tanggal_mulai = data['tanggalMulaiEmailBlast']
        tanggal_selesai = data['tanggalSelesaiEmailBlast']
        path_file = data['pathFile']
        periode = data['periodeBlastE']
        status = 'S'
        id_email_blast = data['idEmailBlast']

        print(f"✏️ Update EmailBlast dengan ID: {id_email_blast}")

        # 1. Set idDetailEmail ke NULL di lulusan
        cursor.execute("""
            UPDATE lulusan
            SET idDetailEmail = NULL
            WHERE idDetailEmail IN (
                SELECT idDetailEmail FROM detailemailblast WHERE idEmailBlast = %s
            )
        """, (id_email_blast,))

        # 2. Hapus detail email blast lama
        cursor.execute("DELETE FROM detailemailblast WHERE idEmailBlast = %s", (id_email_blast,))

        # 3. Update master email blast TANPA targetTahunLulusan
        cursor.execute("""
            UPDATE emailblast SET 
                namaEmailBlast=%s, tanggalMulaiEmailBlast=%s, tanggalSelesaiEmailBlast=%s,
                statusEmailBlast=%s, subjek=%s, isiEmail=%s, pathFile=%s, periodeBlastE=%s 
            WHERE idEmailBlast=%s
        """, (
            nama_email_blast, tanggal_mulai, tanggal_selesai,
            status, subject, isi_email, path_file, periode, id_email_blast
        ))

        conn.commit()
        print("✅ Data EmailBlast berhasil di-update !")
        return jsonify({"message": "EmailBlast berhasil di-update "}), 200

    except Exception as e:
        print("🔥 ERROR:", str(e))
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def run_blast_job(data):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        tahun_lulus = int(data['targetTahunLulusan']) - 1
        subject = data['subjek']
        nama_email_blast = data['namaEmailBlast']
        isi_email = data['isiEmail']
        tanggal_mulai = data['tanggalMulaiEmailBlast']
        tanggal_selesai = data['tanggalSelesaiEmailBlast']
        path_file = data['pathFile']
        periode = data['periodeBlastE']
        status = 'S'

        cursor.execute("SELECT nama, email FROM lulusan WHERE tahunLulus = %s AND email IS NOT NULL AND email != ''", (tahun_lulus,))
        users = cursor.fetchall()

        id_email_blast = generate_id_email_blast(cursor)

        for user in users:
            email = user['email']
            name = user['nama']
            body = isi_email.replace('{nama}', name)

            try:
                send_email(email, subject, body)
            except Exception as e:
                print(f"❌ Gagal kirim ke {email}: {e}")
                status = 'G'

        cursor.execute("""
            INSERT INTO emailblast 
            (idEmailBlast, namaEmailBlast, tanggalMulaiEmailBlast, tanggalSelesaiEmailBlast, targetTahunLulusan, 
             statusEmailBlast, subjek, isiEmail, pathFile, periodeBlastE) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            id_email_blast, nama_email_blast, tanggal_mulai, tanggal_selesai, tahun_lulus,
            status, subject, isi_email, path_file, periode
        ))

        for user in users:
            id_detail = generate_id_detail_email(cursor)
            cursor.execute("""
                INSERT INTO detailemailblast (idDetailEmail, idEmailBlast, statusPengirimanE)
                VALUES (%s, %s, %s)
            """, (id_detail, id_email_blast, status))

            cursor.execute("""
                UPDATE lulusan
                SET idDetailEmail = %s
                WHERE nama = %s AND email = %s AND tahunLulus = %s
            """, (id_detail, user['nama'], user['email'], tahun_lulus))

        conn.commit()
        print("✅ Email blast berhasil dikirim dan disimpan!")
        return jsonify({"message": "Email blast berhasil dikirim dan disimpan!"})
    except Exception as e:
        print("🔥 ERROR:", str(e))
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
