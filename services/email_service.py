import mysql.connector
import smtplib
from flask import request, jsonify, copy_current_request_context
from datetime import datetime, timedelta
from scheduler import scheduler  # scheduler instance dari file scheduler.py
import os
import uuid
import time
import json
import yaml
import traceback
from werkzeug.utils import secure_filename
from email.message import EmailMessage
from config import SMTP_SERVER, SMTP_PORT, GMAIL_USER, GMAIL_PASS, DB_CONFIG, UPLOAD_FOLDER
from utils import allowed_file

class EmailService:
    def __init__(self, scheduler):
        self.scheduler = scheduler

    def send_email(self, to, subject, body, attachment=None):
        if not to or '@' not in to:
            print(f"❌ Email tujuan tidak valid: {to}")
            return False

        msg = EmailMessage()
        msg['Subject'] = subject or "Tanpa Subjek"
        msg['From'] = GMAIL_USER
        msg['To'] = to
        msg.set_content(body)  # Plain text

        if attachment:
            if os.path.isfile(attachment):
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
            else:
                print(f"⚠️ Attachment tidak ditemukan: {attachment}")

        try:
            print(f"📤 Menghubungi server SMTP ({SMTP_SERVER}:{SMTP_PORT})...")
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
                smtp.starttls()
                smtp.login(GMAIL_USER, GMAIL_PASS)
                smtp.send_message(msg)
                print(f"✅ Email berhasil dikirim ke {to}")
                return True
        except smtplib.SMTPAuthenticationError as e:
            print(f"❌ Autentikasi Gagal: Cek GMAIL_USER dan GMAIL_PASS. Error: {e}")
        except smtplib.SMTPException as e:
            print(f"❌ Gagal kirim email (SMTP error): {e}")
        except Exception as e:
            print(f"❌ Error umum saat mengirim ke {to}: {e}")

        return False



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
            periode_list = data.get('periodeBlastE', [])
            if isinstance(periode_list, str):
                periode_list = [periode_list]

            now = datetime.now()
            messages = []

            for idx, periode_str in enumerate(periode_list):
                scheduled_time = datetime.strptime(periode_str, "%Y-%m-%d %H:%M:%S")

                if scheduled_time > now:
                    print(f"⏳ Menjadwalkan email ke-{idx+1} pada:", scheduled_time)

                    @copy_current_request_context
                    def scheduled_job():
                        self.run_blast_job(data, insert=False)

                    self.scheduler.add_job(
                        scheduled_job,
                        trigger='date',
                        run_date=scheduled_time,
                        id=f"job_{scheduled_time.strftime('%Y%m%d%H%M%S')}_{data['namaEmailBlast']}_{idx}",
                        replace_existing=False
                    )

                    if idx == 0:
                        try:
                            tanggal_selesai = datetime.strptime(data['tanggalSelesaiEmailBlast'], "%Y-%m-%d")
                            if now > tanggal_selesai:
                                print("📭 Melewati tanggal selesai, tidak menjadwalkan email bulanan.")
                                messages.append(f"Email ke-{idx+1} dijadwalkan, bulanan dihentikan karena sudah lewat tanggal selesai")
                                continue
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
                        messages.append(f"Email ke-{idx+1} dijadwalkan dan bulanan setelahnya.")
                    else:
                        messages.append(f"Email ke-{idx+1} dijadwalkan pada {scheduled_time}")

                else:
                    self.run_blast_job(data, insert=False)
                    messages.append(f"Email ke-{idx+1} sudah lewat waktunya dan langsung dikirim.")

            return jsonify({"message": messages}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 400


    def get_email_blast_by_id(self, id_email_blast):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            cursor.execute("SELECT * FROM emailblast WHERE idEmailBlast = %s", (id_email_blast,))
            result = cursor.fetchone()

            return jsonify(result), 200 if result else (jsonify({"error": "EmailBlast tidak ditemukan"}), 404)

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
        data = request.form.to_dict(flat=False)  # penting: flat=False untuk ambil multiple periode
        print("DATA DITERIMA:", data)

        try:
            periode_list = data.get('periodeBlastE', [])
            if isinstance(periode_list, str):
                periode_list = [periode_list]

            # Simpan data ke DB dan dapatkan id_email_blast
            result = self.run_blast_job_custompesan(data)
            if isinstance(result, tuple):
                response_data = result[0].json
                id_email_blast = data.get('idEmailBlast') or response_data.get('idEmailBlast')
            else:
                id_email_blast = data.get('idEmailBlast')

            if not id_email_blast:
                return jsonify({"error": "idEmailBlast tidak ditemukan setelah simpan"}), 500

            now = datetime.now()
            job_ids = []

            for periode_str in periode_list:
                try:
                    scheduled_time = datetime.strptime(periode_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return jsonify({"error": f"Format waktu salah: {periode_str}"}), 400

                if scheduled_time > now:
                    print(f"⏳ Menjadwalkan email pada: {scheduled_time}")

                    @copy_current_request_context
                    def scheduled_job_clone(data_clone=data.copy(), waktu=periode_str):
                        data_clone['periodeBlastE'] = waktu
                        self.run_blast_job_custompesan(data_clone)

                    job_id = f"custom_{id_email_blast}_{scheduled_time.strftime('%Y%m%d%H%M%S')}"
                    scheduler.add_job(
                        scheduled_job_clone,
                        trigger='date',
                        run_date=scheduled_time,
                        id=job_id,
                        replace_existing=True
                    )
                    job_ids.append(job_id)

            return jsonify({"message": "Email blast tersimpan & dijadwalkan", "jobs": job_ids}), 200

        except Exception as e:
            print("‼️ ERROR SCHEDULER:")
            import traceback
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

        
    
    def run_blast_job_custompesan(self, data):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # ✅ Ambil data dengan pengecekan apakah berupa list
            def safe_get(field):
                val = data.get(field)
                return val[0] if isinstance(val, list) else val

            tahun_lulus = int(safe_get('targetTahunLulusan'))
            subject = safe_get('subjek')
            nama_email_blast = safe_get('namaEmailBlast')
            isi_email = safe_get('isiEmail') or ''
            periode = safe_get('periodeBlastE')
            id_email_blast = safe_get('idEmailBlast')

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
                if isinstance(path_file, list):
                    path_file = path_file[0]  # handle jika list

            # 🔍 Ambil user alumni dari tahun lulus
            cursor.execute("""
                SELECT nama, email 
                FROM lulusan 
                WHERE tahunLulus = %s 
                AND email IS NOT NULL 
                AND email != ''
            """, (tahun_lulus,))
            users = cursor.fetchall()

            # 🔁 Insert atau Update data email blast
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
            data = request.get_json()

            idEmailBlast = data['idEmailBlast']
            namaEmailBlast = data['namaEmailBlast']
            tanggalMulai = data['tanggalMulaiEmailBlast']
            tanggalSelesai = data['tanggalSelesaiEmailBlast']
            targetTahunLulusan = data['targetTahunLulusan']
            subjek = data.get('subjek', '')
            isiEmail = data.get('isiEmail', '')
            pathFile = data.get('pathFile', '')

            # ✅ Konversi ke string JSON untuk disimpan ke DB
            periodeBlastE = json.dumps(data['periodeBlastE'])

            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()

            update_query = """
                UPDATE emailblast SET
                    namaEmailBlast = %s,
                    tanggalMulaiEmailBlast = %s,
                    tanggalSelesaiEmailBlast = %s,
                    targetTahunLulusan = %s,
                    subjek = %s,
                    isiEmail = %s,
                    pathFile = %s,
                    periodeBlastE = %s
                WHERE idEmailBlast = %s
            """

            cursor.execute(update_query, (
                namaEmailBlast,
                tanggalMulai,
                tanggalSelesai,
                targetTahunLulusan,
                subjek,
                isiEmail,
                pathFile,
                periodeBlastE,
                idEmailBlast
            ))

            conn.commit()
            return jsonify({"message": "Email blast berhasil diupdate"}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500

        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
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

            # 🔽 Baca template email dari file YAML
            with open('template_email.yaml', 'r', encoding='utf-8') as f:
                template_data = yaml.safe_load(f)
            isi_email_template = template_data.get('isi_email_template', '')

            cursor.execute("""
                SELECT l.nama, l.email, l.jenisKelamin, l.username, l.password, p.namaProdi 
                FROM lulusan l
                LEFT JOIN prodi p ON l.idProdi = p.idProdi
                WHERE l.tahunLulus = %s AND l.email IS NOT NULL AND l.email != ''
            """, (tahun_lulus,))
            users = cursor.fetchall()

            if insert:
                id_email_blast = self.generate_id_email_blast(cursor)

                cursor.execute("""
                    INSERT INTO emailblast 
                    (idEmailBlast, namaEmailBlast, tanggalMulaiEmailBlast, tanggalSelesaiEmailBlast, targetTahunLulusan, 
                    statusEmailBlast, subjek, isiEmail, pathFile, periodeBlastE) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_email_blast, nama_email_blast, tanggal_mulai, tanggal_selesai, tahun_lulus,
                    status_blast, subject, isi_email_template, path_file, json.dumps(periode)
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
            periode_blast_dt = datetime.strptime(data['periodeBlastE'][0], "%Y-%m-%d %H:%M:%S") if isinstance(data['periodeBlastE'], list) else datetime.strptime(data['periodeBlastE'], "%Y-%m-%d %H:%M:%S")
            tanggal_selesai_dt = datetime.strptime(data['tanggalSelesaiEmailBlast'], "%Y-%m-%d")

            if now < (periode_blast_dt + timedelta(days=28)):
                print("⏭ Bulan pertama, belum kirim bulanan.")
                return

            if now.date() > tanggal_selesai_dt.date():
                print("🛑 Melewati tanggal selesai, tidak mengirim email bulanan.")
                return

            tahun_lulus = int(data['targetTahunLulusan'])
            subject = data['subjek']

            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT isiEmail FROM emailblast WHERE namaEmailBlast = %s
            """, (data['namaEmailBlast'],))
            row = cursor.fetchone()
            isi_email_template = row['isiEmail'] if row and row['isiEmail'] else """"""

            cursor.execute("""
                SELECT l.nama, l.email, l.jenisKelamin, l.username, l.password, p.namaProdi, l.statusMengisi
                FROM lulusan l
                LEFT JOIN prodi p ON l.idProdi = p.idProdi
                WHERE l.tahunLulus = %s AND l.email IS NOT NULL AND l.email != ''
            """, (tahun_lulus,))
            users = cursor.fetchall()

            for user in users:
                if user.get('statusMengisi') == 'SG':
                    print(f"⏭ {user['email']} sudah mengisi, skip.")
                    continue

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

            # Ambil data email blast
            cursor.execute("SELECT * FROM emailblast WHERE idEmailBlast = %s", (idEmailBlast,))
            email_blast = cursor.fetchone()
            if not email_blast:
                return jsonify({"error": "Email blast tidak ditemukan."}), 404

            now = datetime.now()
            periode_blast_e = email_blast.get('periodeBlastE')

            # Jika periode_blast_e adalah JSON array string
            active_now = False
            if isinstance(periode_blast_e, str):
                try:
                    periode_list = json.loads(periode_blast_e)
                    if isinstance(periode_list, list):
                        for t in periode_list:
                            try:
                                jadwal_dt = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
                                if now >= jadwal_dt:
                                    active_now = True
                                    break
                            except:
                                continue
                    else:
                        # fallback ke string biasa
                        jadwal_dt = datetime.strptime(periode_blast_e, '%Y-%m-%d %H:%M:%S')
                        if now >= jadwal_dt:
                            active_now = True
                except json.JSONDecodeError:
                    # fallback jika bukan JSON
                    try:
                        jadwal_dt = datetime.strptime(periode_blast_e, '%Y-%m-%d %H:%M:%S')
                        if now >= jadwal_dt:
                            active_now = True
                    except ValueError:
                        return jsonify({"error": "Format tanggal periodeBlastE tidak valid."}), 400

            if not active_now:
                return jsonify({
                    "message": f"Email blast belum dapat diaktifkan karena belum waktunya. Jadwal aktif: {periode_blast_e}"
                }), 200

            if email_blast['statusEmailBlast'] != 'A':
                tahun_lulus = email_blast['targetTahunLulusan']
                cursor.execute("""
                    SELECT l.nama, l.email, l.jenisKelamin, l.username, l.password, p.namaProdi,l.tahunLulus 
                    FROM lulusan l
                    LEFT JOIN prodi p ON l.idProdi = p.idProdi
                    WHERE l.tahunLulus = %s AND l.email IS NOT NULL AND l.email != ''
                """, (tahun_lulus,))
                users = cursor.fetchall()

                isi_email_template = email_blast['isiEmail'] or (
                    ""
                )
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

                    try:
                        body = isi_email_template.format(
                            sapaan=sapaan,
                            nama=name,
                            prodi=nama_prodi,
                            username=username,
                            password=password
                        )
                    except KeyError as ke:
                        pesan_logs.append(f"Template error: variabel tidak ditemukan - {str(ke)}")
                        continue

                    try:
                        if file_ada:
                            self.send_email(email, subject, body, attachment=full_attachment_path)
                        else:
                            self.send_email(email, subject, body)
                        pesan_logs.append(f"127.0.0.1 - - [{now_str}] BERHASIL mengirimkan ke {email} (Tahun Lulus: {user.get('tahunLulus', 'Tidak Diketahui')})")
                    except Exception as e:
                        pesan_logs.append(f"127.0.0.1 - - [{now_str}] GAGAL mengirim ke {email}: {str(e)}")

                # Update status
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
            print("‼️ ERROR SERVER 500:")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()

   
    def nonaktifkan_email_blast(self, idEmailBlast):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # Set status jadi Nonaktif
            cursor.execute("""
                UPDATE emailblast SET statusEmailBlast = 'N' WHERE idEmailBlast = %s
            """, (idEmailBlast,))
            conn.commit()

            # Hapus semua job APScheduler yang terkait idEmailBlast
            removed_jobs = []
            for job in scheduler.get_jobs():
                if idEmailBlast in job.id:
                    scheduler.remove_job(job.id)
                    removed_jobs.append(job.id)

            print(f"🛑 Scheduler dibatalkan: {removed_jobs}")

            return jsonify({
                "message": "Email blast berhasil dinonaktifkan dan semua scheduler dibatalkan.",
                "jobs_dihapus": removed_jobs
            })

        except Exception as e:
            print("‼️ ERROR saat nonaktifkan:")
            import traceback
            traceback.print_exc()
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
                SELECT l.nim, l.nama, l.email, l.jenisKelamin, l.username, l.password, p.namaProdi, l.tahunLulus
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
                else:
                    now_str = datetime.now().strftime('%d/%b/%Y %H:%M:%S')
                    log_messages.append(
                        f"127.0.0.1 - - [{now_str}] BERHASIL mengirimkan ke {email} (Tahun Lulus: {alumni.get('tahunLulus', 'Tidak Diketahui')})"
                    )

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


    def get_tahun_lulus_plulusan(self):
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT cohort FROM lulusan WHERE cohort IS NOT NULL ORDER BY cohort DESC")
        result = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify(result)

        
    def blast_email_plulusan(self, data):
        print("DATA DITERIMA:", data)

        try:
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

        # 🟢 SIMPAN SELALU ke emailblast, meskipun status 'N'
        idEmailBlast = self.run_blast_job_plulusan(data, insert=True)
        data['idEmailBlast'] = idEmailBlast  # ⬅️ agar bisa dipakai jika perlu

        # Cek status
        if data['statusEmailBlast'] == 'N':
            print("⛔ Status Nonaktif, tidak menjadwalkan blast.")
            return jsonify({"message": "Email blast disimpan dalam status Nonaktif"}), 200

        # Kalau aktif, lanjutkan ke scheduler
        try:
            scheduled_time = datetime.strptime(data['periodeBlastE'], "%Y-%m-%d %H:%M:%S")
            now = datetime.now()

            if scheduled_time > now:
                print("⏳ Menjadwalkan email pertama pada:", scheduled_time)

                @copy_current_request_context
                def scheduled_job():
                    self.run_blast_job_plulusan(data, insert=False)

                self.scheduler.add_job(
                    scheduled_job,
                    trigger='date',
                    run_date=scheduled_time,
                    id=f"job_{scheduled_time.strftime('%Y%m%d%H%M%S')}_{data['namaEmailBlast']}",
                    replace_existing=True
                )

                return jsonify({"message": f"Email pertama dijadwalkan pada {scheduled_time}."}), 200

            else:
                return self.run_blast_job_plulusan(data, insert=False)

        except Exception as e:
            return jsonify({"error": str(e)}), 400

    def run_blast_job_plulusan(self, data, insert=False):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            status_blast = data.get('statusEmailBlast')
            subject = data['subjek']
            nama_email_blast = data['namaEmailBlast']
            tanggal_mulai = data['tanggalMulaiEmailBlast']
            tanggal_selesai = data['tanggalSelesaiEmailBlast']
            path_file = data['pathFile']
            periode = data['periodeBlastE']
            cohort = int(data.get('targetTahunLulusan', 0))
            tahun_lulus = cohort - 1

            # 🔽 Load template dari file YAML
            with open('template_email_plulusan.yaml', 'r', encoding='utf-8') as f:
                template_data = yaml.safe_load(f)
            isi_email_template = template_data.get('isi_email_template', '')

            if insert:
                id_email_blast = self.generate_id_email_blast(cursor)

                cursor.execute("""
                    INSERT INTO emailblast 
                    (idEmailBlast, namaEmailBlast, tanggalMulaiEmailBlast, tanggalSelesaiEmailBlast, 
                    targetTahunLulusan, statusEmailBlast, subjek, isiEmail, pathFile, periodeBlastE) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_email_blast,
                    nama_email_blast,
                    tanggal_mulai,
                    tanggal_selesai,
                    str(cohort),
                    status_blast,
                    subject,
                    isi_email_template,
                    path_file,
                    periode
                ))

                cursor.execute("SELECT NIM FROM lulusan WHERE tahunLulus = %s", (tahun_lulus,))
                nim_list = [row['NIM'] for row in cursor.fetchall()]
                print(f"🔍 Ditemukan {len(nim_list)} NIM dari tahun lulus {tahun_lulus}")

                total_detail = 0
                used_id_plulusan = set()

                for nim in nim_list:
                    cursor.execute("SELECT idPLulusan FROM lulusan_penggunalulusan_tablerelasi WHERE NIM = %s", (nim,))
                    id_rows = cursor.fetchall()

                    for row in id_rows:
                        id_plulusan = row['idPLulusan']
                        used_id_plulusan.add(id_plulusan)
                        id_detail = self.generate_idDetailEmail(cursor)

                        cursor.execute("""
                            INSERT INTO detailemailblast (idDetailEmail, idEmailBlast)
                            VALUES (%s, %s)
                        """, (id_detail, id_email_blast))
                        total_detail += 1

                for id_pl in used_id_plulusan:
                    cursor.execute("""
                        UPDATE penggunalulusan
                        SET idEmailBlast = %s
                        WHERE idPLulusan = %s
                    """, (id_email_blast, id_pl))

                conn.commit()
                print(f"✅ EmailBlast {id_email_blast} disimpan dengan {total_detail} detail email")
                return id_email_blast

            # 🔽 Pengiriman email
            if 'idEmailBlast' not in data:
                return jsonify({"error": "idEmailBlast tidak ditemukan"}), 400

            if status_blast == 'A':
                cursor.execute("""
                    SELECT p.email, p.namaPLulusan, p.namaPerusahaan 
                    FROM detailemailblast d
                    JOIN penggunalulusan p ON d.idEmailBlast = %s AND p.idEmailBlast = d.idEmailBlast
                    WHERE p.email IS NOT NULL AND p.email != ''
                """, (data['idEmailBlast'],))
                users = cursor.fetchall()

                for user in users:
                    email = user['email']
                    name = user['namaPLulusan']
                    perusahaan = user.get('namaPerusahaan', '-')

                    cursor.execute("SELECT alamatPerusahaan, jenisKelamin FROM penggunalulusan WHERE email = %s", (email,))
                    extra_info = cursor.fetchone()
                    lokasi = extra_info.get('alamatPerusahaan', '-') if extra_info else '-'
                    jk = extra_info.get('jenisKelamin', 'L') if extra_info else 'L'
                    panggilan = 'Saudara' if jk == 'L' else 'Saudari'

                    cursor.execute("""
                        SELECT l.nama
                        FROM lulusan_penggunalulusan_tablerelasi rel
                        JOIN lulusan l ON rel.NIM = l.NIM
                        JOIN penggunalulusan p ON p.idPLulusan = rel.idPLulusan
                        WHERE p.email = %s
                    """, (email,))
                    lulusan_rows = cursor.fetchall()
                    daftar_lulusan = "\n- " + "\n- ".join([r['nama'] for r in lulusan_rows]) if lulusan_rows else "-"

                    body = isi_email_template.format(
                        Panggilan=panggilan,
                        Nama=name,
                        NamaPsh=perusahaan,
                        LokasiPsh=lokasi,
                        daftarLulusan=daftar_lulusan
                    )

                    try:
                        self.send_email(email, subject, body)
                        print(f"📤 Email dikirim ke {email}")
                    except Exception as e:
                        print(f"❌ Gagal kirim ke {email}: {e}")
            else:
                print("⛔ Status Nonaktif, tidak menjadwalkan blast.")

            return jsonify({"message": "Email blast diproses."})

        except Exception as e:
            print(f"❌ Exception: {e}")
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    
    def generate_idDetailEmail(self, cursor):
        cursor.execute("""
            SELECT idDetailEmail 
            FROM detailemailblast 
            WHERE idDetailEmail LIKE 'DEM%%'
            ORDER BY idDetailEmail DESC 
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if result:
            last_id = result['idDetailEmail']
            try:
                last_number = int(last_id.replace('DEM', ''))
                new_number = last_number + 1
            except ValueError:
                new_number = 1
        else:
            new_number = 1

        return f"DEM{new_number:03d}"


    def generate_id_email_blast_plulusan(self, cursor): 
        cursor.execute("SELECT MAX(idEmailBlast) AS max_id FROM emailblast")
        result = cursor.fetchone()['max_id']
        if result and result.startswith("EM"):
            try:
                number = int(result.replace("EM", "")) + 1
            except ValueError:
                number = 1
        else:
            number = 1
        return f"EM{number:03d}"



    def aktifkan_email_blast_plulusan(self, idEmailBlast):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # Ambil data email blast
            cursor.execute("SELECT * FROM emailblast WHERE idEmailBlast = %s", (idEmailBlast,))
            email_blast = cursor.fetchone()
            if not email_blast:
                return jsonify({"error": "Email blast tidak ditemukan."}), 404

            now = datetime.now()
            periode_blast_e = email_blast.get('periodeBlastE')

            if isinstance(periode_blast_e, str):
                try:
                    periode_blast_e = datetime.strptime(periode_blast_e, "%Y-%m-%d %H:%M:%S")
                except:
                    return jsonify({"error": "Format periodeBlastE tidak valid."}), 400

            if periode_blast_e and now < periode_blast_e:
                cursor.execute("UPDATE emailblast SET statusEmailBlast = 'N' WHERE idEmailBlast = %s", (idEmailBlast,))
                conn.commit()
                return jsonify({
                    "message": f"⏳ Email blast belum aktif. Tunggu hingga {periode_blast_e.strftime('%Y-%m-%d %H:%M:%S')}."
                }), 200

            if email_blast['statusEmailBlast'] != 'A':
                cursor.execute("""
                    SELECT idPLulusan, namaPLulusan, namaPerusahaan, alamatPerusahaan, email, jenisKelamin
                    FROM penggunalulusan 
                    WHERE idEmailBlast = %s AND email IS NOT NULL AND email != ''
                """, (idEmailBlast,))
                users = cursor.fetchall()

                isi_email_template = email_blast.get('isiEmail') or """"""

                subject = email_blast['subjek']
                attachment_path = email_blast.get('pathFile')
                full_attachment_path = os.path.join(os.getcwd(), attachment_path) if attachment_path else None
                file_ada = full_attachment_path and os.path.isfile(full_attachment_path)
                pesan_logs = []

                for user in users:
                    now_str = datetime.now().strftime('%d/%b/%Y %H:%M:%S')
                    email = user['email'].strip()
                    name = user.get('namaPLulusan', '').strip()
                    perusahaan = user.get('namaPerusahaan', '-')
                    lokasi = user.get('alamatPerusahaan', '-')
                    jk = user.get('jenisKelamin', 'L')
                    panggilan = 'Saudara' if jk == 'L' else 'Saudari'
                    id_plulusan = user.get('idPLulusan')

                    if not name or '@' not in email or '.' not in email:
                        pesan_logs.append(f"[{now_str}] ❌ EMAIL TIDAK VALID: {email} ({name})")
                        continue

                    # Ambil daftar lulusan dari relasi idPLulusan
                    cursor.execute("""
                        SELECT l.nama
                        FROM lulusan_penggunalulusan_tablerelasi r
                        JOIN lulusan l ON r.NIM = l.NIM
                        WHERE r.idPLulusan = %s
                    """, (id_plulusan,))
                    lulusan_rows = cursor.fetchall()

                    if not lulusan_rows:
                        daftar_lulusan = "- (Tidak ada data lulusan untuk dinilai)"
                    elif len(lulusan_rows) == 1:
                        daftar_lulusan = f"- {lulusan_rows[0]['nama']}"
                    else:
                        daftar_lulusan = "\n" + "\n".join([f"{i+1}. {r['nama']}" for i, r in enumerate(lulusan_rows)])

                    body = isi_email_template.format(
                        Panggilan=panggilan,
                        Nama=name,
                        NamaPsh=perusahaan,
                        LokasiPsh=lokasi,
                        daftarLulusan=daftar_lulusan
                    )

                    try:
                        if file_ada:
                            self.send_email(email, subject, body, attachment=full_attachment_path)
                        else:
                            self.send_email(email, subject, body)
                        pesan_logs.append(f"[{now_str}] ✅ Email terkirim ke {email}")
                    except Exception as e:
                        pesan_logs.append(f"[{now_str}] ❌ GAGAL kirim ke {email}: {str(e)}")

                cursor.execute("UPDATE emailblast SET statusEmailBlast = 'A' WHERE idEmailBlast = %s", (idEmailBlast,))

                cursor.execute("SELECT COUNT(*) AS total FROM emailblastinglog")
                total_logs = cursor.fetchone()['total'] + 1
                id_log = f"log00{total_logs}"

                cursor.execute("""
                    INSERT INTO emailblastinglog (
                        idLog, idEmailBlast, namaBlast, tanggalMulai, tanggalSelesai, jumlah, status, pesanLogging
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_log,
                    idEmailBlast,
                    email_blast['namaEmailBlast'],
                    email_blast['tanggalMulaiEmailBlast'],
                    email_blast['tanggalSelesaiEmailBlast'],
                    len(users),
                    'S',
                    "\n".join(pesan_logs)
                ))

                conn.commit()
                return jsonify({"message": "✅ Email blast berhasil dikirim."}), 200

            else:
                return jsonify({"message": "ℹ️ Email blast sudah aktif sebelumnya."}), 200

        except Exception as e:
            print(f"❌ ERROR: {str(e)}")
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def nonaktifkan_email_blast_plulusan(self, idEmailBlast):
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

    def blast_email_pesan_plulusan(self):
        data = request.form.to_dict()
        print("DATA DITERIMA:", data)
        try:
            scheduled_time = datetime.strptime(data['periodeBlastE'], "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            if scheduled_time > now:
                print("⏳ Menjadwalkan email pada:", scheduled_time)
                @copy_current_request_context
                def scheduled_job():
                    self.run_blast_job_custompesan_Plulusan(data)
                return jsonify({"message": f"Email dijadwalkan pada {scheduled_time}"}), 200
            else:
                return self.run_blast_job_custompesan_Plulusan(data)
        except Exception as e:
            return jsonify({"error": f"Format periodeBlastE salah: {str(e)}"}), 400
        
    def run_blast_job_custompesan_Plulusan(self, data):
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

    def blast_email_update_only_Plulusan(self):
        data = request.json
        print("DATA DITERIMA UNTUK UPDATE:", data)
        if 'idEmailBlast' not in data:
            return jsonify({"error": "idEmailBlast wajib diisi untuk update"}), 400
        try:
            return self.update_plulusan(data)
        except Exception as e:
            return jsonify({"error": f"Gagal update: {str(e)}"}), 500
        

    def update_plulusan(self, data):
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
            periodeBlastE = data['periodeBlastE']

            cursor.execute("""
                UPDATE emailblast SET 
                    namaEmailBlast=%s, 
                    tanggalMulaiEmailBlast=%s, 
                    tanggalSelesaiEmailBlast=%s,
                    subjek=%s, 
                    isiEmail=%s, 
                    pathFile=%s,
                    periodeBlastE=%s, 
                    targetTahunLulusan=%s
                WHERE idEmailBlast=%s
            """, (
                nama_email_blast, tanggal_mulai, tanggal_selesai,
                subject, isi_email, path_file, periodeBlastE, target_lulus, id_email_blast
            ))

            conn.commit()
            return jsonify({"message": "EmailBlast berhasil di-update"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()



    def manual_blast_plulusan(self,idEmailBlast):
        conn = None
        cursor = None
        try:
            selected_ids = request.json.get('selectedPLulusan', [])
            if not selected_ids:
                return jsonify({"message": "❌ Tidak ada pengguna lulusan yang dipilih."}), 400

            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # Get email blast config
            cursor.execute("SELECT * FROM emailblast WHERE idEmailBlast = %s", (idEmailBlast,))
            email_blast = cursor.fetchone()
            if not email_blast:
                return jsonify({"message": "❌ Email blast tidak ditemukan."}), 404

            template = email_blast.get('isiEmail') or """"""

            subject = email_blast.get('subjek') or 'Tanpa Subjek'
            attachment_path = email_blast.get('pathFile')
            full_attachment_path = os.path.join(os.getcwd(), attachment_path) if attachment_path else None
            file_ada = full_attachment_path and os.path.isfile(full_attachment_path)

            sukses, gagal = 0, 0
            pesan_logs = []
            waktu_mulai = datetime.now()

            # Ambil data pengguna lulusan yang dipilih
            format_ids = ','.join(['%s'] * len(selected_ids))
            cursor.execute(f"""
                SELECT idPLulusan, namaPLulusan, namaPerusahaan, alamatPerusahaan, email, jenisKelamin
                FROM penggunalulusan
                WHERE idPLulusan IN ({format_ids})
            """, tuple(selected_ids))

            users = cursor.fetchall()

            for user in users:
                now_str = datetime.now().strftime('%d/%b/%Y %H:%M:%S')
                email = user.get('email', '').strip()
                name = user.get('namaPLulusan', '').strip()
                perusahaan = user.get('namaPerusahaan', '-')
                lokasi = user.get('alamatPerusahaan', '-')
                jk = user.get('jenisKelamin', 'L')
                panggilan = 'Saudara' if jk == 'L' else 'Saudari'
                id_plulusan = user.get('idPLulusan')

                # Skip jika email tidak valid
                if not name or '@' not in email or '.' not in email:
                    pesan_logs.append(f"[{now_str}] ❌ EMAIL TIDAK VALID: {email} ({name})")
                    gagal += 1
                    continue

                # Ambil daftar lulusan dari relasi idPLulusan
                cursor.execute("""
                    SELECT l.nama
                    FROM lulusan_penggunalulusan_tablerelasi r
                    JOIN lulusan l ON r.NIM = l.NIM
                    WHERE r.idPLulusan = %s
                """, (id_plulusan,))
                lulusan_rows = cursor.fetchall()

                if not lulusan_rows:
                    daftar_lulusan = "- (Tidak ada data lulusan untuk dinilai)"
                elif len(lulusan_rows) == 1:
                    daftar_lulusan = f"- {lulusan_rows[0]['nama']}"
                else:
                    daftar_lulusan = "\n" + "\n".join([f"{i+1}. {r['nama']}" for i, r in enumerate(lulusan_rows)])

                body = template.format(
                    Panggilan=panggilan,
                    Nama=name,
                    NamaPsh=perusahaan,
                    LokasiPsh=lokasi,
                    daftarLulusan=daftar_lulusan
                )

                try:
                    if file_ada:
                         self.send_email(email, subject, body, attachment=full_attachment_path)
                    else:
                         self.send_email(email, subject, body)
                    sukses += 1
                    pesan_logs.append(f"[{now_str}] ✅ Email terkirim ke {email}")
                except Exception as e:
                    gagal += 1
                    pesan_logs.append(f"[{now_str}] ❌ GAGAL kirim ke {email}: {str(e)}")

            waktu_selesai = datetime.now()

            # Buat ID log baru
            cursor.execute("SELECT COUNT(*) AS total FROM emailblastinglog")
            total_logs = cursor.fetchone()['total'] + 1
            id_log = f"log00{total_logs}"

            cursor.execute("""
                INSERT INTO emailblastinglog (
                    idLog, idEmailBlast, namaBlast, tanggalMulai, tanggalSelesai, jumlah, status, pesanLogging
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                id_log,
                idEmailBlast,
                f"Manual Blast Pengguna Lulusan - {waktu_mulai.strftime('%Y-%m-%d %H:%M:%S')}",
                waktu_mulai,
                waktu_selesai,
                len(users),
                'S' if sukses > 0 else 'G',
                "\n".join(pesan_logs)
            ))

            conn.commit()

            return jsonify({
                "message": "✅ Manual email blast selesai.",
                "total": len(users),
                "berhasil": sukses,
                "gagal": gagal,
                "log": pesan_logs
            }), 200

        except Exception as e:
            if conn: conn.rollback()
            traceback.print_exc()
            return jsonify({"message": f"❌ Server error: {str(e)}"}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
