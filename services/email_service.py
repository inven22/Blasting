import mysql.connector
import smtplib
from flask import request, jsonify, copy_current_request_context
from datetime import datetime, timedelta
import os
import uuid
import traceback
from werkzeug.utils import secure_filename
from email.message import EmailMessage
from config import SMTP_SERVER, SMTP_PORT, GMAIL_USER, GMAIL_PASS, DB_CONFIG, UPLOAD_FOLDER
from utils import allowed_file

class EmailService:
    def __init__(self, scheduler):
        self.scheduler = scheduler

    def send_email(self, to, subject, body, attachment=None):
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = GMAIL_USER
        msg['To'] = to
        msg.set_content(body)

        if attachment and os.path.isfile(attachment):
            try:
                with open(attachment, 'rb') as f:
                    file_data = f.read()
                    file_name = os.path.basename(attachment)
                    msg.add_attachment(
                        file_data,
                        maintype='application',
                        subtype='octet-stream',
                        filename=file_name
                    )
                    print(f"📎 Melampirkan file: {file_name}")
            except Exception as e:
                print(f"⚠️ Gagal melampirkan file '{attachment}': {e}")
                # Tetap lanjut kirim email tanpa lampiran
                pass

        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
                smtp.starttls()
                smtp.login(GMAIL_USER, GMAIL_PASS)
                smtp.send_message(msg)
                print(f"📧 Email berhasil dikirim ke {to}")
                return True  # ✅ Berhasil kirim
        except Exception as e:
            print(f"❌ Gagal mengirim email ke {to}: {e}")
            return False  # ❌ Gagal kirim


    def generate_id_email_blast(self, cursor):
        cursor.execute("SELECT idEmailBlast FROM emailblast WHERE idEmailBlast LIKE 'EM%' ORDER BY idEmailBlast DESC LIMIT 1")
        last_id = cursor.fetchone()
        number = int(last_id['idEmailBlast'][2:]) + 1 if last_id else 1
        return f"EM{str(number).zfill(3)}"

    def generate_id_detail_email(self, cursor):
        cursor.execute("SELECT idDetailEmail FROM detailemailblast WHERE idDetailEmail LIKE 'DEM%' ORDER BY idDetailEmail DESC LIMIT 1")
        last_id = cursor.fetchone()
        number = int(last_id['idDetailEmail'][3:]) + 1 if last_id else 1
        return f"DEM{str(number).zfill(3)}"

    def get_tahun_lulus(self):
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT tahunLulus FROM lulusan WHERE tahunLulus IS NOT NULL ORDER BY tahunLulus DESC")
        result = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify(result)

    def blast_email(self, data):
        print("DATA DITERIMA:", data)

        if data['statusEmailBlast'] == 'N':
            print("⛔ Status Nonaktif, tidak menjadwalkan blast.")
            return jsonify({"message": "Email blast disimpan dalam status Nonaktif"}), 200

        try:
            # ✅ Validasi nama blast terlebih dahulu
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM emailblast WHERE namaEmailBlast = %s", (data['namaEmailBlast'],))
            (count,) = cursor.fetchone()
            if count > 0:
                return jsonify({"error": "Nama email blast sudah ada"}), 400
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

        # ✅ Simpan ke DB hanya sekali
        self.run_blast_job(data, insert=True)

        try:
            scheduled_time = datetime.strptime(data['periodeBlastE'], "%Y-%m-%d %H:%M:%S")
            now = datetime.now()

            if scheduled_time > now:
                print("⏳ Menjadwalkan email pertama pada:", scheduled_time)

                @copy_current_request_context
                def scheduled_job():
                    self.run_blast_job(data, insert=False)

                self.scheduler.add_job(
                    scheduled_job,
                    trigger='date',
                    run_date=scheduled_time,
                    id=f"job_{scheduled_time.strftime('%Y%m%d%H%M%S')}_{data['namaEmailBlast']}",
                    replace_existing=True
                )

                # ⏲ Jadwalkan bulanan (jika belum lewat tanggal selesai)
                try:
                    tanggal_selesai = datetime.strptime(data['tanggalSelesaiEmailBlast'], "%Y-%m-%d")
                    if now > tanggal_selesai:
                        print("📭 Melewati tanggal selesai, tidak menjadwalkan email bulanan.")
                        return jsonify({"message": "Email pertama dijadwalkan, bulanan dihentikan karena sudah lewat tanggal selesai"}), 200
                except ValueError:
                    return jsonify({"error": "Format tanggalSelesaiEmailBlast harus 'YYYY-MM-DD'"}), 400

                @copy_current_request_context
                def monthly_job():
                    self.send_monthly_email_job(data)

                self.scheduler.add_job(
                    monthly_job,
                    trigger='cron',
                    day=scheduled_time.day,
                    hour=scheduled_time.hour,
                    minute=scheduled_time.minute,
                    id=f"monthly_{scheduled_time.strftime('%d%H%M')}_{data['namaEmailBlast']}_only",
                    replace_existing=True
                )

                print(f"📆 Email bulanan dijadwalkan tiap tanggal {scheduled_time.day} jam {scheduled_time.hour}:{scheduled_time.minute}")
                return jsonify({"message": f"Email pertama dijadwalkan pada {scheduled_time} dan bulanan setelahnya."}), 200

            else:
                # Waktu sudah lewat, langsung kirim tanpa simpan ulang
                return self.run_blast_job(data, insert=False)

        except Exception as e:
            return jsonify({"error": str(e)}), 400



    def get_email_blast_by_id(self, idEmailBlast):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM emailblast WHERE idEmailBlast = %s", (idEmailBlast,))
            result = cursor.fetchone()
            if not result:
                return jsonify({"error": "Data tidak ditemukan"}), 404
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def get_email_blast_by_id_pesan(self, idEmailBlast):
        return self.get_email_blast_by_id(idEmailBlast)

    def blast_email_update_only(self):
        data = request.json
        print("DATA DITERIMA UNTUK UPDATE:", data)
        if 'idEmailBlast' not in data:
            return jsonify({"error": "idEmailBlast wajib diisi untuk update"}), 400
        try:
            return self.update(data)
        except Exception as e:
            return jsonify({"error": f"Gagal update: {str(e)}"}), 500

    def blast_email_pesan(self):
        data = request.form.to_dict()
        print("DATA DITERIMA:", data)
        try:
            scheduled_time = datetime.strptime(data['periodeBlastE'], "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            if scheduled_time > now:
                print("⏳ Menjadwalkan email pada:", scheduled_time)
                @copy_current_request_context
                def scheduled_job():
                    self.run_blast_job_custompesan(data)
                return jsonify({"message": f"Email dijadwalkan pada {scheduled_time}"}), 200
            else:
                return self.run_blast_job_custompesan(data)
        except Exception as e:
            return jsonify({"error": f"Format periodeBlastE salah: {str(e)}"}), 400

    def run_blast_job_custompesan(self, data):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            tahun_lulus = int(data['targetTahunLulusan']) 
            subject = data['subjek']
            nama_email_blast = data['namaEmailBlast']
            isi_email = data['isiEmail']
            periode = data['periodeBlastE']
            id_email_blast = data.get('idEmailBlast')

            uploaded_file = request.files.get('file')
            path_file = None

            if uploaded_file and uploaded_file.filename != '':
                if allowed_file(uploaded_file.filename):
                    filename = f"{uuid.uuid4().hex}_{secure_filename(uploaded_file.filename)}"
                    save_path = os.path.join(UPLOAD_FOLDER, filename)
                    uploaded_file.save(save_path)
                    path_file = save_path.replace("\\", "/")
                else:
                    return jsonify({"error": "Jenis file tidak diizinkan"}), 400
            else:
                path_file = data.get('pathFile', '')

            cursor.execute("""
                SELECT nama, email 
                FROM lulusan 
                WHERE tahunLulus = %s 
                AND email IS NOT NULL 
                AND email != ''
            """, (tahun_lulus,))
            users = cursor.fetchall()

            if id_email_blast:
                cursor.execute("""
                    UPDATE emailblast SET 
                        namaEmailBlast=%s, 
                        targetTahunLulusan=%s, 
                        subjek=%s, 
                        isiEmail=%s, 
                        pathFile=%s
                    WHERE idEmailBlast=%s
                """, (
                    nama_email_blast, tahun_lulus, 
                    subject, isi_email, path_file, id_email_blast
                ))
            else:
                id_email_blast = self.generate_id_email_blast(cursor)
                cursor.execute("""
                    INSERT INTO emailblast 
                    (idEmailBlast, namaEmailBlast, targetTahunLulusan, 
                    subjek, isiEmail, pathFile, periodeBlastE) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_email_blast, nama_email_blast, tahun_lulus, 
                    subject, isi_email, path_file, periode
                ))

            conn.commit()
            return jsonify({"message": "Data email blast tersimpan tanpa mengirim email!"})
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def update(self, data):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            subject = data['subjek']
            nama_email_blast = data['namaEmailBlast']
            isi_email = data['isiEmail']
            tanggal_mulai = data['tanggalMulaiEmailBlast']
            tanggal_selesai = data['tanggalSelesaiEmailBlast']
            path_file = data['pathFile']
            target_lulus = data['targetTahunLulusan']
            id_email_blast = data['idEmailBlast']

            cursor.execute("""
                UPDATE emailblast SET 
                    namaEmailBlast=%s, 
                    tanggalMulaiEmailBlast=%s, 
                    tanggalSelesaiEmailBlast=%s,
                    subjek=%s, 
                    isiEmail=%s, 
                    pathFile=%s, 
                    targetTahunLulusan=%s
                WHERE idEmailBlast=%s
            """, (
                nama_email_blast, tanggal_mulai, tanggal_selesai,
                subject, isi_email, path_file, target_lulus, id_email_blast
            ))

            conn.commit()
            return jsonify({"message": "EmailBlast berhasil di-update"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def run_blast_job(self, data, insert=False):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            status_blast = data.get('statusEmailBlast')
            tahun_lulus = int(data['targetTahunLulusan'])
            subject = data['subjek']
            nama_email_blast = data['namaEmailBlast']
            tanggal_mulai = data['tanggalMulaiEmailBlast']
            tanggal_selesai = data['tanggalSelesaiEmailBlast']
            path_file = data['pathFile']
            periode = data['periodeBlastE']
            status = 'G'

            cursor.execute("""
                SELECT l.nama, l.email, l.jenisKelamin, l.username, l.password, p.namaProdi 
                FROM lulusan l
                LEFT JOIN prodi p ON l.idProdi = p.idProdi
                WHERE l.tahunLulus = %s AND l.email IS NOT NULL AND l.email != ''
            """, (tahun_lulus,))
            users = cursor.fetchall()

            isi_email_template = """
    Alumni POLBAN yang kami banggakan,

Apa kabar {sapaan} {nama}, Alumni {prodi}? Semoga {sapaan} senantiasa dalam keadaan sehat, sejahtera, dan bahagia. Aamiin YRA.

Perkenalkan, kami dari Tim Tracer Study POLBAN tahun 2023. Maksud kami menghubungi {sapaan} {nama} adalah untuk mengajak berpartisipasi dalam kegiatan tracer study (penelusuran alumni) yang rutin dilakukan tiap tahun oleh POLBAN.

Tracer study merupakan kegiatan pengumpulan informasi rekam jejak lulusan, untuk mengetahui transisi kompetensi yang dimiliki lulusan dari pendidikan tinggi menuju dunia kerja, serta menggali perolehan kompetensi dan jalur karir lulusan. Oleh karena itu, POLBAN melakukan survei kepada lulusan dan pengguna lulusan. {sapaan} {nama} diminta sebagai responden pengisi kuesioner tracer study. Penelusuran dilakukan secara online melalui tautan berikut:

http://penelusuranalumni.polban.ac.id

Data yang berhasil dikumpulkan dari tracer study POLBAN akan digunakan untuk kebutuhan evaluasi diri, re-akreditasi institusi dan prodi, serta sebagai acuan peningkatan mutu lulusan agar memiliki kompetensi yang handal dan profesional.

Periode pengisian kuesioner tracer study oleh lulusan dilaksanakan mulai hari ini sampai dengan 31 Oktober 2023. Mengingat pentingnya informasi yang diberikan untuk pengembangan institusi, jurusan, dan program studi, kami sangat mengharapkan data dan informasi tersebut dapat diterima sebelum tanggal tersebut.

Berikut data akun Anda:

Username : {username}
Password : {password}

Setelah berhasil login, silakan segera ubah password serta periksa data diri pada menu Profile dan Edit Profile.

Jika mengalami kesulitan saat login, Anda dapat menghubungi surveyor atau mengirim email ke tracer.study@polban.ac.id.

Atas perhatian dan partisipasi {sapaan}, kami ucapkan terima kasih.

Salam,  
Tim Tracer Study POLBAN 2023  
Politeknik Negeri Bandung
    """

            if insert:
                id_email_blast = self.generate_id_email_blast(cursor)

                cursor.execute("""
                    INSERT INTO emailblast 
                    (idEmailBlast, namaEmailBlast, tanggalMulaiEmailBlast, tanggalSelesaiEmailBlast, targetTahunLulusan, 
                    statusEmailBlast, subjek, isiEmail, pathFile, periodeBlastE) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_email_blast, nama_email_blast, tanggal_mulai, tanggal_selesai, tahun_lulus,
                    status_blast, subject, isi_email_template, path_file, periode
                ))

                for user in users:
                    id_detail = self.generate_id_detail_email(cursor)
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

            # ✅ Kirim email jika status aktif
            if status_blast == 'A':
                for user in users:
                    email = user['email']
                    name = user['nama']
                    jenis_kelamin = user.get('jenisKelamin', '').upper()
                    username = user.get('username', '')
                    password = user.get('password', '')
                    nama_prodi = user.get('namaProdi', 'Prodi')

                    sapaan = 'Saudara' if jenis_kelamin == 'L' else 'Saudari' if jenis_kelamin == 'P' else 'Saudara/Saudari'

                    body = isi_email_template.format(
                        sapaan=sapaan,
                        nama=name,
                        prodi=nama_prodi,
                        username=username,
                        password=password
                    )

                    try:
                        self.send_email(email, subject, body)
                    except Exception as e:
                        print(f"❌ Gagal kirim ke {email}: {e}")

            else:
                print("⛔ Status Nonaktif, tidak kirim email.")

            return jsonify({"message": "Email blast diproses."})

        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def send_monthly_email_job(self, data): 
        conn = None
        cursor = None
        try:
            now = datetime.now()
            periode_blast_dt = datetime.strptime(data['periodeBlastE'], "%Y-%m-%d %H:%M:%S")
            tanggal_selesai_dt = datetime.strptime(data['tanggalSelesaiEmailBlast'], "%Y-%m-%d")

            # ⏭ Skip jika belum lewat 1 bulan dari periode pertama
            if now < (periode_blast_dt + timedelta(days=28)):
                print("⏭ Bulan pertama, belum kirim bulanan.")
                return

            # 🛑 Stop jika sudah lewat tanggal selesai
            if now.date() > tanggal_selesai_dt.date():
                print("🛑 Melewati tanggal selesai, tidak mengirim email bulanan.")
                return

            tahun_lulus = int(data['targetTahunLulusan'])
            subject = data['subjek']

            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT l.nama, l.email, l.jenisKelamin, l.username, l.password, p.namaProdi 
                FROM lulusan l
                LEFT JOIN prodi p ON l.idProdi = p.idProdi
                WHERE l.tahunLulus = %s AND l.email IS NOT NULL AND l.email != ''
            """, (tahun_lulus,))
            users = cursor.fetchall()

            isi_email_template = """
    Alumni POLBAN yang kami banggakan,

    Apa kabar {sapaan} {nama}, Alumni {prodi}? Semoga {sapaan} senantiasa dalam keadaan sehat, sejahtera, dan bahagia. Aamiin YRA.

    Perkenalkan, kami dari Tim Tracer Study POLBAN tahun 2023. Maksud kami menghubungi {sapaan} {nama} adalah untuk mengajak berpartisipasi dalam kegiatan tracer study (penelusuran alumni) yang rutin dilakukan tiap tahun oleh POLBAN.

    Tracer study merupakan kegiatan pengumpulan informasi rekam jejak lulusan, untuk mengetahui transisi kompetensi yang dimiliki lulusan dari pendidikan tinggi menuju dunia kerja, serta menggali perolehan kompetensi dan jalur karir lulusan. Oleh karena itu, POLBAN melakukan survei kepada lulusan dan pengguna lulusan. {sapaan} {nama} diminta sebagai responden pengisi kuesioner tracer study. Penelusuran dilakukan secara online melalui tautan berikut:

    http://penelusuranalumni.polban.ac.id

    Data yang berhasil dikumpulkan dari tracer study POLBAN akan digunakan untuk kebutuhan evaluasi diri, re-akreditasi institusi dan prodi, serta sebagai acuan peningkatan mutu lulusan agar memiliki kompetensi yang handal dan profesional.

    Periode pengisian kuesioner tracer study oleh lulusan dilaksanakan mulai hari ini sampai dengan 31 Oktober 2023. Mengingat pentingnya informasi yang diberikan untuk pengembangan institusi, jurusan, dan program studi, kami sangat mengharapkan data dan informasi tersebut dapat diterima sebelum tanggal tersebut.

    Berikut data akun Anda:

    Username : {username}
    Password : {password}

    Setelah berhasil login, silakan segera ubah password serta periksa data diri pada menu Profile dan Edit Profile.

    Jika mengalami kesulitan saat login, Anda dapat menghubungi surveyor atau mengirim email ke tracer.study@polban.ac.id.

    Atas perhatian dan partisipasi {sapaan}, kami ucapkan terima kasih.

    Salam,  
    Tim Tracer Study POLBAN 2023  
    Politeknik Negeri Bandung
            """

            for user in users:
                email = user['email']
                name = user['nama']
                jenis_kelamin = user.get('jenisKelamin', '').upper()
                username = user.get('username', '')
                password = user.get('password', '')
                nama_prodi = user.get('namaProdi', 'Prodi')

                sapaan = 'Saudara' if jenis_kelamin == 'L' else 'Saudari' if jenis_kelamin == 'P' else 'Saudara/Saudari'

                body = isi_email_template.format(
                    sapaan=sapaan,
                    nama=name,
                    prodi=nama_prodi,
                    username=username,
                    password=password
                )

                try:
                    self.send_email(email, subject, body)
                    print(f"📨 Email terkirim ke {email}")
                except Exception as e:
                    print(f"❌ Gagal kirim ke {email}: {e}")

            print("✅ Pengiriman bulanan selesai.")
        except Exception as e:
            print(f"❌ Error pengiriman bulanan: {e}")
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    

    def aktifkan_email_blast(self, idEmailBlast):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM emailblast WHERE idEmailBlast = %s", (idEmailBlast,))
            email_blast = cursor.fetchone()
            if not email_blast:
                return jsonify({"error": "Email blast tidak ditemukan."}), 404

            # Ambil waktu sekarang dan waktu pengiriman
            now = datetime.now()
            periode_blast_e = email_blast.get('periodeBlastE')

            # Jika belum waktunya
            if periode_blast_e and now < periode_blast_e:
                cursor.execute("UPDATE emailblast SET statusEmailBlast = 'N' WHERE idEmailBlast = %s", (idEmailBlast,))
                conn.commit()
                return jsonify({
                    "message": "Email blast ditunda karena belum waktunya,Lihat waktu periodeBlastE untuk mengaktifkan ."
                }), 200

            # Jika sudah waktunya dan belum aktif
            if email_blast['statusEmailBlast'] != 'A':
                tahun_lulus = email_blast['targetTahunLulusan']
                cursor.execute("""
                    SELECT l.nama, l.email, l.jenisKelamin, l.username, l.password, p.namaProdi 
                    FROM lulusan l
                    LEFT JOIN prodi p ON l.idProdi = p.idProdi
                    WHERE l.tahunLulus = %s AND l.email IS NOT NULL AND l.email != ''
                """, (tahun_lulus,))
                users = cursor.fetchall()

                isi_email_template = email_blast['isiEmail']
                subject = email_blast['subjek']
                attachment_path = email_blast.get('pathFile')
                full_attachment_path = os.path.join(os.getcwd(), attachment_path) if attachment_path else None
                file_ada = full_attachment_path and os.path.isfile(full_attachment_path)

                pesan_logs = []

                for user in users:
                    now_str = datetime.now().strftime('%d/%b/%Y %H:%M:%S')
                    email = user.get('email', '').strip()
                    name = user.get('nama', '').strip()
                    jenis_kelamin = (user.get('jenisKelamin') or '').upper()
                    username = user.get('username', '')
                    password = user.get('password', '')
                    nama_prodi = user.get('namaProdi', 'Prodi')

                    if not name or '@' not in email or '.' not in email:
                        pesan_logs.append(f"127.0.0.1 - - [{now_str}] EMAIL TIDAK VALID atau NAMA TIDAK DITEMUKAN: {email} ({name})")
                        continue

                    sapaan = 'Saudara' if jenis_kelamin == 'L' else 'Saudari' if jenis_kelamin == 'P' else 'Saudara/Saudari'
                    body = isi_email_template.format(
                        sapaan=sapaan,
                        nama=name,
                        prodi=nama_prodi,
                        username=username,
                        password=password
                    )
                    try:
                        if file_ada:
                            self.send_email(email, subject, body, attachment=full_attachment_path)
                        else:
                            self.send_email(email, subject, body)
                        pesan_logs.append(f"127.0.0.1 - - [{now_str}] BERHASIL mengirimkan ke {email}")
                    except Exception as e:
                        pesan_logs.append(f"127.0.0.1 - - [{now_str}] GAGAL mengirim ke {email}: {str(e)}")

                cursor.execute("UPDATE emailblast SET statusEmailBlast = 'A' WHERE idEmailBlast = %s", (idEmailBlast,))
                cursor.execute("UPDATE detailemailblast SET statusPengirimanE = 'S' WHERE idEmailBlast = %s", (idEmailBlast,))

                cursor.execute("SELECT COUNT(*) AS jumlah FROM lulusan WHERE tahunLulus = %s AND email IS NOT NULL AND email != ''", (tahun_lulus,))
                jumlah_lulusan = cursor.fetchone()['jumlah']

                cursor.execute("SELECT COUNT(*) AS total FROM emailblastinglog")
                total_logs = cursor.fetchone()['total'] + 1
                id_log = f"log00{total_logs}"

                nama_blast = email_blast['namaEmailBlast']
                tanggal_mulai = email_blast['tanggalMulaiEmailBlast']
                tanggal_selesai = email_blast['tanggalSelesaiEmailBlast']
                pesan_logging = "\n".join(pesan_logs)

                cursor.execute("""
                    INSERT INTO emailblastinglog (
                        idLog, idEmailBlast, namaBlast, tanggalMulai, tanggalSelesai, jumlah, status, pesanLogging
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_log,
                    email_blast['idEmailBlast'],
                    nama_blast,
                    tanggal_mulai,
                    tanggal_selesai,
                    jumlah_lulusan,
                    'S',
                    pesan_logging
                ))

                conn.commit()
                return jsonify({"message": "Email blast berhasil dikirim karena sudah waktunya."}), 200

            else:
                return jsonify({"message": "Email blast sudah aktif dan sebelumnya telah dikirim."}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()





    def nonaktifkan_email_blast(self, idEmailBlast):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE emailblast SET statusEmailBlast = 'N' WHERE idEmailBlast = %s
            """, (idEmailBlast,))
            conn.commit()
            return jsonify({"message": "Email blast berhasil dinonaktifkan."})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def manual_blast(self, idEmailBlast):
        try:
            selected_nims = request.json.get('selectedNIMs', [])
            if not selected_nims:
                return jsonify({"message": "Tidak ada alumni yang dipilih."}), 400

            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            waktu_mulai = datetime.now()

            # Ambil data email blast
            cursor.execute("""
                SELECT subjek, isiEmail, pathFile
                FROM emailblast
                WHERE idEmailBlast = %s
            """, (idEmailBlast,))
            blast = cursor.fetchone()

            if not blast:
                return jsonify({"message": "Email blast tidak ditemukan"}), 404

            subject = blast.get('subjek') or 'Tanpa Subjek'
            template = blast.get('isiEmail') or ''
            attachment_path = blast.get('pathFile')

            # Ambil data lulusan dari NIM terpilih
            format_nim_placeholders = ','.join(['%s'] * len(selected_nims))
            cursor.execute(f"""
                SELECT l.nim, l.nama, l.email, l.jenisKelamin, l.username, l.password, p.namaProdi
                FROM lulusan l
                LEFT JOIN prodi p ON l.idProdi = p.idProdi
                WHERE l.nim IN ({format_nim_placeholders})
            """, tuple(selected_nims))

            alumni_rows = cursor.fetchall()
            results = []
            log_messages = []

            # Ambil semua idDetailEmail
            cursor.execute("""
                SELECT idDetailEmail
                FROM detailemailblast
                WHERE idEmailBlast = %s
                ORDER BY idDetailEmail ASC
            """, (idEmailBlast,))
            detail_entries = cursor.fetchall()

            for i, alumni in enumerate(alumni_rows):
                if i >= len(detail_entries):
                    break  # Hindari index error

                detail = detail_entries[i]
                email = alumni.get('email', '').strip()
                if not email:
                    log_messages.append(f"❗ Email tidak ditemukan untuk NIM {alumni['nim']}")
                    continue

                sapaan = (alumni.get('jenisKelamin') or '').strip().upper()
                sapaan = 'Saudara' if sapaan == 'L' else 'Saudari' if sapaan == 'P' else 'Saudara/Saudari'

                body = template \
                    .replace('{sapaan}', sapaan) \
                    .replace('{nama}', alumni.get('nama', '')) \
                    .replace('{username}', alumni.get('username', '')) \
                    .replace('{password}', alumni.get('password', '')) \
                    .replace('{prodi}', alumni.get('namaProdi', ''))

                sent = self.send_email(
                    to=email,
                    subject=subject,
                    body=body,
                    attachment=attachment_path
                )
                print(f"DEBUG: hasil send_email untuk {email} = {sent}")

                status = 'S' if sent else 'G'

                cursor.execute("""
                    UPDATE detailemailblast
                    SET statusPengirimanE = %s
                    WHERE idDetailEmail = %s
                """, (status, detail['idDetailEmail']))

                results.append({
                    "nim": alumni['nim'],
                    "nama": alumni['nama'],
                    "email": email,
                    "status": status
                })

                if not sent:
                    log_messages.append(f"❌ Gagal kirim ke {email} (NIM {alumni['nim']})")

            waktu_selesai = datetime.now()

            # Buat ID log unik
            cursor.execute("SELECT idLog FROM emailblastinglog ORDER BY idLog DESC LIMIT 1")
            last_log = cursor.fetchone()
            new_number = int(last_log['idLog'][3:]) + 1 if last_log else 1
            new_idLog = f"log{new_number:03d}"

            status_log = 'S' if any(r['status'] == 'S' for r in results) else 'G'
            full_pesan = "\n".join(log_messages) if log_messages else "✅ Semua email terkirim sukses."

            cursor.execute("""
                INSERT INTO emailblastinglog (idLog, idEmailBlast, namaBlast, tanggalMulai, tanggalSelesai, jumlah, status, pesanLogging)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                new_idLog,
                idEmailBlast,
                f"Manual Blast {waktu_mulai.strftime('%Y-%m-%d %H:%M:%S')}",
                waktu_mulai,
                waktu_selesai,
                len(results),
                status_log,
                full_pesan
            ))

            conn.commit()

            return jsonify({
                "message": "Email blast berhasil.",
                "results": results
            })

        except Exception as e:
            conn.rollback()
            print("❌ ERROR:", str(e))
            traceback.print_exc()
            return jsonify({"message": f"Server error: {str(e)}"}), 500

        finally:
            if cursor: cursor.close()
            if conn: conn.close()