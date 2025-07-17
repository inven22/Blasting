import mysql.connector
from flask import jsonify
from datetime import datetime
import requests
import traceback
import re
import time
import json
import yaml
from flask import request, jsonify
from flask import copy_current_request_context
from config import DB_CONFIG, ID_INSTANCE, API_TOKEN

class WhatsAppBlastService:
    def __init__(self, scheduler):
        self.scheduler = scheduler

    def get_tahun_lulus_wa(self):
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT tahunLulus FROM lulusan WHERE tahunLulus IS NOT NULL ORDER BY tahunLulus DESC")
        result = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify(result)

    def blast_wa(self, data):
        print("📥 DATA DITERIMA:", data)

        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM wablast WHERE namaBlast = %s", (data['namaBlast'],))
            (count,) = cursor.fetchone()
            if count > 0:
                return jsonify({"error": "Nama WA blast sudah ada"}), 400
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            conn.close()

        if data['StatusBlast2'] == 'N':
            print("⛔ Status Nonaktif, tidak menjadwalkan WA blast.")
            try:
                return self.run_wa_blast(data, insert=True)
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        # Eksekusi langsung + penjadwalan untuk semua periode
        self.run_wa_blast(data, insert=True)

        try:
            periode_list = json.loads(data['periodeBlast']) if isinstance(data['periodeBlast'], str) else data['periodeBlast']
            now = datetime.now()

            for i, periode_str in enumerate(periode_list):
                scheduled_time = datetime.strptime(periode_str, "%Y-%m-%d %H:%M:%S")
                job_id = f"job_{scheduled_time.strftime('%Y%m%d%H%M')}_{data['namaBlast']}_{i}"

                if scheduled_time > now:
                    print(f"⏳ Menjadwalkan WA ke-{i+1} pada: {scheduled_time}")

                    @copy_current_request_context
                    def scheduled_job():
                        self.run_wa_blast(data, insert=False)

                    self.scheduler.add_job(
                        scheduled_job,
                        trigger='date',
                        run_date=scheduled_time,
                        id=job_id,
                        replace_existing=True
                    )
                else:
                    print(f"⏱ {periode_str} sudah lewat, dilewati.")

            return jsonify({"message": f"{len(periode_list)} WA dijadwalkan."}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 400

            
    def run_wa_blast(self, data, insert=False):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            tahun_lulus = int(data['targetType2'])
            status_blast = data['StatusBlast2']
            tanggal_mulai = data['TanggalBlast2']
            tanggal_selesai = data['tanggalSelesaiBlast']
            nama_blast = data['namaBlast']
            periode_list = json.loads(data['periodeBlast']) if isinstance(data['periodeBlast'], str) else data['periodeBlast']
            status = 'G'

            # 🔽 Membaca template dari file YAML
            with open('template_wa.yaml', 'r', encoding='utf-8') as f:
                template_data = yaml.safe_load(f)
            template_pesan = template_data.get('template_pesan', '')

            cursor.execute("""
                SELECT l.nama, l.noWa, l.nim, l.jenisKelamin, l.idProdi, l.tahunLulus, p.namaProdi 
                FROM lulusan l
                JOIN prodi p ON l.idProdi = p.idProdi
                WHERE l.tahunLulus = %s AND l.noWa IS NOT NULL AND l.noWa != ''
            """, (tahun_lulus,))
            users = cursor.fetchall()

            if insert:
                id_wablast = self.generate_id_wablast(cursor)
                print("📌 ID WA Blast:", id_wablast)

                cursor.execute("""
                    INSERT INTO wablast 
                    (idWaBlast, namaBlast, targetType2, targetId2, TanggalBlast2, StatusBlast2, 
                    tanggalSelesaiBlast, isiPesan, periodeBlast)
                    VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s)
                """, (
                    id_wablast, nama_blast, tahun_lulus, tanggal_mulai, status_blast,
                    tanggal_selesai, template_pesan, json.dumps(periode_list)
                ))

                for user in users:
                    id_detail = self.generate_id_detail_wa(cursor)
                    cursor.execute("""
                        INSERT INTO detailwablast (idDetailWa, idWaBlast, statusPengiriman)
                        VALUES (%s, %s, %s)
                    """, (id_detail, id_wablast, status))

                    cursor.execute("""
                        UPDATE lulusan SET idDetailWa = %s WHERE nim = %s
                    """, (id_detail, user['nim']))

                conn.commit()

            if status_blast == 'A':
                current_hour = datetime.now().hour
                salam = ("Selamat Pagi" if 4 <= current_hour < 12 else
                        "Selamat Siang" if 12 <= current_hour < 15 else
                        "Selamat Sore" if 15 <= current_hour < 18 else
                        "Selamat Malam")

                for user in users:
                    jk = (user.get('jenisKelamin') or '').strip().upper()
                    panggilan = 'Saudara' if jk == 'L' else 'Saudari' if jk == 'P' else 'Rekan'

                    final_msg = template_pesan \
                        .replace("{salam_waktu}", salam) \
                        .replace("{Panggilan}", panggilan) \
                        .replace("{Nama}", user['nama']) \
                        .replace("{Prodi}", user['namaProdi']) \
                        .replace("{TahunLulus}", str(user['tahunLulus']))

                    nomor = user['noWa']
                    chat_id = f"{nomor}@c.us"
                    self.send_wa(chat_id, final_msg)

            return jsonify({"message": "✅ WA blast diproses."})
        except Exception as e:
            print("🔥 ERROR run_wa_blast:", str(e))
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            conn.close() 

    def aktifkan_wa_blast(self, idWaBlast): 
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # Ambil data WA blast
            cursor.execute("SELECT * FROM wablast WHERE idWaBlast = %s", (idWaBlast,))
            wa_blast = cursor.fetchone()
            if not wa_blast:
                return jsonify({"error": "WA blast tidak ditemukan."}), 404

            now = datetime.now()
            periode_blast = None
            raw_periode = wa_blast.get('periodeBlast')

            try:
                import json
                if isinstance(raw_periode, str):
                    decoded = json.loads(raw_periode)
                    if isinstance(decoded, list) and len(decoded) > 0:
                        decoded = sorted(decoded)
                        periode_blast = datetime.strptime(decoded[0], '%Y-%m-%d %H:%M:%S')
                    else:
                        periode_blast = datetime.strptime(raw_periode, '%Y-%m-%d %H:%M:%S')
            except Exception:
                return jsonify({"error": "Format periodeBlast tidak valid atau tidak bisa di-parse."}), 400

            if periode_blast and now < periode_blast:
                return jsonify({
                    "message": f"WA blast belum dapat diaktifkan karena belum waktunya. Jadwal aktif: {periode_blast.strftime('%Y-%m-%d %H:%M:%S')}."
                }), 200

            if wa_blast['StatusBlast2'] != 'A':
                tahun_lulus = wa_blast['targetType2']
                cursor.execute("""
                    SELECT l.nama, l.noWa, l.jenisKelamin, l.nim, l.username, l.password, l.tahunLulus, p.namaProdi 
                    FROM lulusan l
                    JOIN prodi p ON l.idProdi = p.idProdi
                    WHERE l.tahunLulus = %s AND l.noWa IS NOT NULL AND l.noWa != ''
                """, (tahun_lulus,))
                users = cursor.fetchall()

                # Tentukan salam waktu
                jam_sekarang = now.hour
                if 4 <= jam_sekarang < 12:
                    salam_waktu = "Selamat Pagi"
                elif 12 <= jam_sekarang < 15:
                    salam_waktu = "Selamat Siang"
                elif 15 <= jam_sekarang < 18:
                    salam_waktu = "Selamat Sore"
                else:
                    salam_waktu = "Selamat Malam"

                # Ambil template dari database
                raw_template = wa_blast.get('isiPesan')
                isi_pesan_template = raw_template.strip() if raw_template else (
                    ""
                )

                import re
                pesan_logs = []

                for user in users:
                    nama = user['nama']
                    no_wa = user['noWa']
                    jenis_kelamin = (user['jenisKelamin'] or '').strip().upper()
                    prodi = user['namaProdi']
                    tahun_lulus_user = user['tahunLulus']
                    username = user['username'] or '-'
                    password = user['password'] or '-'

                    if not nama or not no_wa:
                        pesan_logs.append(
                            f"127.0.0.1 - - [{now.strftime('%d/%b/%Y %H:%M:%S')}] ❌ INVALID DATA: {nama} / {no_wa}"
                        )
                        continue

                    sapaan = 'Saudara' if jenis_kelamin == 'L' else 'Saudari' if jenis_kelamin == 'P' else 'Saudara/Saudari'

                    # Format pesan
                    pesan_final = isi_pesan_template \
                        .replace("{salam_waktu}", salam_waktu) \
                        .replace("{Panggilan}", sapaan) \
                        .replace("{Nama}", nama) \
                        .replace("{Prodi}", prodi or '-') \
                        .replace("{TahunLulus}", str(tahun_lulus_user or '-')) \
                        .replace("{username}", username) \
                        .replace("{password}", password)

                    pesan_final = re.sub(r"\{[^\}]+\}", "", pesan_final)

                    try:
                        chat_id = f"{no_wa}@c.us"
                        self.send_long_wa(chat_id, pesan_final)
                        pesan_logs.append(
                            f"127.0.0.1 - - [{now.strftime('%d/%b/%Y %H:%M:%S')}] ✅ Terkirim ke {nama} (WA: {no_wa}, Lulus: {tahun_lulus_user})"
                        )
                    except Exception as e:
                        pesan_logs.append(
                            f"127.0.0.1 - - [{now.strftime('%d/%b/%Y %H:%M:%S')}] ❌ GAGAL ke {no_wa}: {str(e)}"
                        )

                cursor.execute("UPDATE wablast SET StatusBlast2 = 'A' WHERE idWaBlast = %s", (idWaBlast,))
                cursor.execute("UPDATE detailwablast SET statusPengiriman = 'S' WHERE idWaBlast = %s", (idWaBlast,))

                jumlah = len(users)
                cursor.execute("SELECT COUNT(*) AS total FROM wablastinglog")
                total = cursor.fetchone()['total'] + 1
                id_log = f"waglog{str(total).zfill(3)}"

                nama_blast = wa_blast['namaBlast']
                tanggal_mulai = wa_blast['TanggalBlast2']
                tanggal_selesai = wa_blast['tanggalSelesaiBlast']
                log_text = "\n".join(pesan_logs)

                cursor.execute("""
                    INSERT INTO wablastinglog (
                        idWaLog, idWaBlast, namaBlastWa, tanggalMulaiWa, tanggalSelesaiWa, jumlahWa, statusWa, pesanLoggingWa
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_log, idWaBlast, nama_blast, tanggal_mulai, tanggal_selesai, jumlah, 'S', log_text
                ))

                conn.commit()
                return jsonify({"message": "WA blast berhasil dikirim karena sudah waktunya."}), 200

            else:
                return jsonify({"message": "WA blast sudah aktif dan sebelumnya telah dikirim."}), 200

        except Exception as e:
            print("‼️ ERROR SERVER 500:")
            import traceback; traceback.print_exc()
            return jsonify({"error": str(e)}), 500

        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()


    def send_long_wa(self, chat_id, message):
        MAX_CHUNK = 2000
        chunks = [message[i:i+MAX_CHUNK] for i in range(0, len(message), MAX_CHUNK)]
        for idx, chunk in enumerate(chunks, 1):
            self.send_wa(chat_id, chunk)
            print(f"✅ Chunk {idx}/{len(chunks)} terkirim ke {chat_id}")
            time.sleep(1)  # Delay 1 detik antar chunk

    def nonaktifkan_wa_blast(self, idWaBlast):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # Ambil data blast WA
            cursor.execute("SELECT * FROM wablast WHERE idWaBlast = %s", (idWaBlast,))
            blast = cursor.fetchone()

            if not blast:
                return jsonify({"error": "WA blast tidak ditemukan."}), 404

            if blast['StatusBlast2'] == 'N':
                return jsonify({"message": "WA blast sudah dalam status Nonaktif."}), 200

            # Update hanya status di wablast
            cursor.execute("UPDATE wablast SET StatusBlast2 = 'N' WHERE idWaBlast = %s", (idWaBlast,))
            conn.commit()

            # Batalkan semua job terjadwal berdasarkan periodeBlast
            if self.scheduler:
                nama_blast = blast['namaBlast']
                periode_str = blast['periodeBlast']

                try:
                    # Jika data string JSON list, misal '["2025-07-09 12:54:00", "2025-07-15 15:56:00"]'
                    periode_list = json.loads(periode_str)
                    if isinstance(periode_list, str):
                        periode_list = [periode_list]
                except Exception as e:
                    print(f"⚠️ Gagal parsing periodeBlast JSON: {e}")
                    # fallback: coba anggap string tunggal
                    periode_list = [periode_str]

                for waktu_str in periode_list:
                    try:
                        waktu = datetime.strptime(waktu_str, "%Y-%m-%d %H:%M:%S")
                        job_id = f"weekly_{waktu.strftime('%w%H%M')}_{nama_blast}_only"
                        self.scheduler.remove_job(job_id)
                        print(f"🛑 Scheduled job {job_id} dibatalkan.")
                    except Exception as e:
                        print(f"⚠️ Gagal membatalkan job untuk waktu {waktu_str}: {e}")

            return jsonify({"message": "WA blast berhasil dinonaktifkan dan semua jadwal dibatalkan."}), 200

        except Exception as e:
            print("‼️ ERROR saat nonaktifkan WA blast:")
            import traceback; traceback.print_exc()
            return jsonify({"error": str(e)}), 500

        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()

    def get_wa_blast_by_id(self, idWaBlast):
            try:
                conn = mysql.connector.connect(**DB_CONFIG)
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT * FROM wablast WHERE idWaBlast = %s", (idWaBlast,))
                result = cursor.fetchone()
                if not result:
                    return jsonify({"error": "Data tidak ditemukan"}), 404
                return jsonify(result)
            except Exception as e:
                return jsonify({"error": str(e)}), 500
            finally:
                if cursor: cursor.close()
                if conn: conn.close()

    def blast_wa_update_only(self):
        data = request.json
        print("DATA DITERIMA UNTUK UPDATE WA BLAST:", data)
        if 'idWaBlast' not in data:
            return jsonify({"error": "idWaBlast wajib diisi untuk update"}), 400
        try:
            return self.update_wa_blast(data)
        except Exception as e:
            return jsonify({"error": f"Gagal update WA blast: {str(e)}"}), 500

    def update_wa_blast(self, data):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            nama_blast = data.get('namaBlast')
            tanggal_mulai = data.get('TanggalBlast2') or None
            tanggal_selesai = data.get('tanggalSelesaiBlast') or None
            isi_pesan = data.get('isiPesan')
            target_lulus = data.get('targetType2')
            periode_list = data.get('periodeBlast', [])
            id_wablast = data.get('idWaBlast')

            # Simpan periodeBlast sebagai string JSON list
            periode_json = json.dumps(periode_list)

            # Ambil data lama jika tanggal_mulai atau tanggal_selesai kosong/null
            cursor.execute("SELECT TanggalBlast2, tanggalSelesaiBlast FROM wablast WHERE idWaBlast = %s", (id_wablast,))
            existing = cursor.fetchone()
            if existing:
                if not tanggal_mulai:
                    tanggal_mulai = existing.get('TanggalBlast2')
                if not tanggal_selesai:
                    tanggal_selesai = existing.get('tanggalSelesaiBlast')

            cursor.execute("""
                UPDATE wablast SET 
                    namaBlast = %s,
                    TanggalBlast2 = %s,
                    tanggalSelesaiBlast = %s,
                    isiPesan = %s,
                    targetType2 = %s,
                    periodeBlast = %s
                WHERE idWaBlast = %s
            """, (
                nama_blast, tanggal_mulai, tanggal_selesai,
                isi_pesan, target_lulus, periode_json, id_wablast
            ))

            conn.commit()
            return jsonify({"message": "WA Blast berhasil di-update"}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    
    def manual_wa_blast(self, idWaBlast):
        try:
            selected_nims = request.json.get('selectedNIMs', [])
            if not selected_nims:
                return jsonify({"message": "Tidak ada alumni yang dipilih."}), 400

            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            waktu_mulai = datetime.now()

            cursor.execute("""
                SELECT isiPesan FROM wablast WHERE idWaBlast = %s
            """, (idWaBlast,))
            blast = cursor.fetchone()
            if not blast:
                return jsonify({"message": "WA blast tidak ditemukan"}), 404

            template = blast.get('isiPesan') or 'tidak ada templat pesan'

            placeholders = ','.join(['%s'] * len(selected_nims))
            cursor.execute(f"""
                SELECT l.nim, l.nama, l.noWa, l.jenisKelamin, l.username, l.password, p.namaProdi, l.tahunLulus
                FROM lulusan l LEFT JOIN prodi p ON l.idProdi = p.idProdi
                WHERE l.nim IN ({placeholders})
            """, tuple(selected_nims))

            alumni_rows = cursor.fetchall()
            results = []
            success_log_entries = []
            log_messages = []

            for alumni in alumni_rows:
                no_wa = str(alumni.get('noWa') or '').strip()
                if not no_wa:
                    log_messages.append(
                        f"127.0.0.1 - - [{datetime.now().strftime('%d/%b/%Y %H:%M:%S')}] ❗ No WA tidak ditemukan untuk NIM {alumni['nim']}"
                    )
                    continue

                current_hour = datetime.now().hour
                salam_waktu = (
                    "Selamat Pagi" if 4 <= current_hour < 12 else
                    "Selamat Siang" if 12 <= current_hour < 15 else
                    "Selamat Sore" if 15 <= current_hour < 18 else
                    "Selamat Malam"
                )

                jk = (alumni.get('jenisKelamin') or '').strip().upper()
                namapanggilan = 'Saudara' if jk == 'L' else 'Saudari' if jk == 'P' else 'Saudara/Saudari'
                body = template \
                    .replace('{Panggilan}', namapanggilan) \
                    .replace('{Nama}', alumni.get('nama', '')) \
                    .replace('{username}', alumni.get('username', '')) \
                    .replace('{password}', alumni.get('password', '')) \
                    .replace('{Prodi}', alumni.get('namaProdi', '')) \
                    .replace('{TahunLulus}', str(alumni.get('tahunLulus', ''))) \
                    .replace('Selamat Siang', salam_waktu) \
                    .replace('{salam_waktu}', salam_waktu)

                sent_result = self.send_wa(chat_id=f"{no_wa}@c.us", message=body)
                sent = True if sent_result is True else False

                status = 'S' if sent else 'G'
                results.append({
                    "nim": alumni['nim'],
                    "nama": alumni['nama'],
                    "noWa": no_wa,
                    "status": status
                })

                log_time = datetime.now().strftime('%d/%b/%Y %H:%M:%S')

                if sent:
                    success_log_entries.append( 
                        f"127.0.0.1 - - [{log_time}] Terkirim ke {alumni['nama']} dengan nomor whatsapp {no_wa} dan (Tahun Lulus: {alumni.get('tahunLulus', 'Tidak Diketahui')})"
                    )
                else:
                    log_messages.append(
                        f"127.0.0.1 - - [{log_time}] ❌ Gagal kirim ke {no_wa} (NIM {alumni['nim']})"
                    )

            waktu_selesai = datetime.now()

            cursor.execute("SELECT idWaLog FROM wablastinglog ORDER BY idWaLog DESC LIMIT 1")
            last_log = cursor.fetchone()
            import re
            if last_log:
                match = re.search(r'(\d+)$', last_log['idWaLog'])
                new_number = int(match.group(1)) + 1 if match else 1
            else:
                new_number = 1
            new_idLog = f"waglog{new_number:03d}"

            # Gabungkan semua log
            full_pesan = "\n".join(success_log_entries + log_messages)
            if not full_pesan.strip():
                full_pesan = "✅ Semua pesan WA terkirim sukses."

            status_log = 'S' if any(r['status'] == 'S' for r in results) else 'G'

            # Pastikan insert full_pesan sebagai string, bukan panjangnya!
            cursor.execute("""
                INSERT INTO wablastinglog 
                (idWaLog, idWaBlast, namaBlastWa, tanggalMulaiWa, tanggalSelesaiWa, jumlahWa, statusWa, pesanLoggingWa)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                new_idLog,
                idWaBlast,
                f"Manual WA Blast {waktu_mulai.strftime('%Y-%m-%d %H:%M:%S')}",
                waktu_mulai,
                waktu_selesai,
                len(results),
                status_log,
                full_pesan  # ⬅ Ini yang masuk, bukan len(full_pesan)
            ))

            conn.commit()

            return jsonify({
                "message": "WA blast berhasil.",
                "results": results
            })

        except Exception as e:
            conn.rollback()
            print("❌ ERROR:", str(e))
            import traceback
            traceback.print_exc()
            return jsonify({"message": f"Server error: {str(e)}"}), 500

        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def send_wa(self, chat_id, message):
        url = f"https://api.green-api.com/waInstance{ID_INSTANCE}/sendMessage/{API_TOKEN}"
        payload = {"chatId": chat_id, "message": message}
        headers = {'Content-Type': 'application/json'}

        if len(message) > 4000:
            print(f"⚠ Pesan ke {chat_id} terlalu panjang: {len(message)} karakter ➤ tidak dikirim")
            return False  # ⬅ Tambah return False supaya jelas gagal

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            try:
                result = response.json()
            except Exception:
                print(f"⚠ GAGAL PARSE JSON respon dari {chat_id} ➤ Status: {response.status_code} ➤ Body: {response.text}")
                return False  # ⬅ Tambah return False

            if response.status_code == 200 and "idMessage" in result:
                print(f"✅ BERHASIL ke {chat_id} ➤ ID: {result['idMessage']}")
                return True  # ⬅ Return True jika sukses
            else:
                print(f"⚠ GAGAL ke {chat_id} ➤ Status: {response.status_code} ➤ Respon: {result}")
                return False  # ⬅ Return False jika gagal status atau tidak ada idMessage

        except Exception as e:
            print(f"❌ ERROR kirim ke {chat_id}: {str(e)}")
            return False  # ⬅ Return False jika exception

    def generate_id_wablast(self, cursor):
        cursor.execute("SELECT idWaBlast FROM wablast ORDER BY idWaBlast DESC LIMIT 1")
        last = cursor.fetchone()
        if last and last['idWaBlast'].startswith("WA"):
            new_id = int(last['idWaBlast'][2:]) + 1
        else:
            new_id = 1
        return f"WA{str(new_id).zfill(2)}"


    def generate_id_detail_wa(self, cursor):
        cursor.execute("SELECT idDetailWa FROM detailwablast ORDER BY idDetailWa DESC LIMIT 1")
        last = cursor.fetchone()
        if last and last['idDetailWa'].startswith("WA"):
            new_id = int(last['idDetailWa'][2:]) + 1
        else:
            new_id = 1
        return f"WA{str(new_id).zfill(3)}"

    def send_monthly_wa_job(self, data):
        print("📆 Eksekusi WA mingguan...")
        self.run_wa_reminder(data)

    def run_wa_reminder(self, data):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            idWaBlast = data['idWaBlast']
            cursor.execute("SELECT isiPesan, StatusBlast2 FROM wablast WHERE idWaBlast = %s", (idWaBlast,))
            result = cursor.fetchone()
            if not result:
                return jsonify({"error": "WA blast tidak ditemukan"}), 404

            template = result['isiPesan']
            status_blast = result['StatusBlast2']

            if status_blast != 'A':
                return jsonify({"message": "Blast belum aktif. Reminder tidak dikirim."}), 400

            cursor.execute("""
                SELECT l.nim, l.nama, l.jenisKelamin, l.noWa, l.tahunLulus, l.username, l.password, l.statusMengisi,
                    p.namaProdi
                FROM lulusan l
                JOIN prodi p ON l.idProdi = p.idProdi
                JOIN detailwablast d ON l.idDetailWa = d.idDetailWa
                WHERE d.idWaBlast = %s AND d.statusPengisian IS NULL
                AND l.noWa IS NOT NULL AND l.noWa != ''
            """, (idWaBlast,))
            alumni_rows = cursor.fetchall()

            current_hour = datetime.now().hour
            salam = ("Selamat Pagi" if 4 <= current_hour < 12 else
                    "Selamat Siang" if 12 <= current_hour < 15 else
                    "Selamat Sore" if 15 <= current_hour < 18 else
                    "Selamat Malam")

            for alumni in alumni_rows:
                if alumni.get('statusMengisi') == 'SG':
                    continue

                no_wa = alumni['noWa']
                jk = (alumni.get('jenisKelamin') or '').strip().upper()
                panggilan = 'Saudara' if jk == 'L' else 'Saudari' if jk == 'P' else 'Rekan'

                body = template \
                    .replace("{Panggilan}", panggilan) \
                    .replace("{Nama}", alumni['nama']) \
                    .replace("{Prodi}", alumni['namaProdi']) \
                    .replace("{TahunLulus}", str(alumni['tahunLulus'])) \
                    .replace("{username}", alumni.get('username', '')) \
                    .replace("{password}", alumni.get('password', '')) \
                    .replace("{salam_waktu}", salam)

                self.send_wa(chat_id=f"{no_wa}@c.us", message=body)

            return jsonify({"message": f"Reminder dikirim ke {len(alumni_rows)} alumni yang belum mengisi."})
        except Exception as e:
            print("🔥 ERROR run_wa_reminder:", str(e))
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close() 

    
    def get_tahun_lulus_wa_plulusan(self):
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT cohort FROM lulusan WHERE cohort IS NOT NULL ORDER BY cohort DESC")
        result = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify(result)

 
    def blast_wa_plulusan(self, data):
        print("DATA DITERIMA:", data)
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM wablast WHERE namaBlast = %s", (data['namaBlast'],))
            (count,) = cursor.fetchone()
            if count > 0:
                return jsonify({"error": "Nama WA blast sudah ada"}), 400
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            conn.close()

        if data['StatusBlast2'] == 'N':
            print("⛔ Status Nonaktif, tidak menjadwalkan WA blast.")
            try:
                return self.run_wa_blast_plulusan(data, insert=True)
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        self.run_wa_blast_plulusan(data, insert=True)

        try:
            # Mendukung multiple periodeBlast (list of string datetime)
            if isinstance(data['periodeBlast'], str):
                periode_list = [data['periodeBlast']]
            elif isinstance(data['periodeBlast'], list):
                periode_list = data['periodeBlast']
            else:
                return jsonify({"error": "Format periodeBlast tidak valid"}), 400

            for waktu_str in periode_list:
                scheduled_time = datetime.strptime(waktu_str, "%Y-%m-%d %H:%M:%S")
                now = datetime.now()

                if scheduled_time > now:
                    print("⏳ Menjadwalkan WA pada:", scheduled_time)

                    @copy_current_request_context
                    def scheduled_job():
                        self.run_wa_blast_plulusan(data, insert=False)

                    self.scheduler.add_job(
                        scheduled_job,
                        trigger='date',
                        run_date=scheduled_time,
                        id=f"job_{scheduled_time.strftime('%Y%m%d%H%M%S')}_{data['namaBlast']}",
                        replace_existing=True
                    )

                    try:
                        tanggal_selesai = datetime.strptime(data['tanggalSelesaiBlast'], "%Y-%m-%d")
                        if now > tanggal_selesai:
                            print("📭 Lewat tanggal selesai, tidak jadwalkan mingguan.")
                            continue
                    except ValueError:
                        return jsonify({"error": "Format tanggalSelesaiBlast harus 'YYYY-MM-DD'"}), 400

                    @copy_current_request_context
                    def weekly_job():
                        self.send_monthly_wa_job_plulusan(data)

                    scheduled_weekday = scheduled_time.weekday()
                    self.scheduler.add_job(
                        weekly_job,
                        trigger='cron',
                        day_of_week=scheduled_weekday,
                        hour=scheduled_time.hour,
                        minute=scheduled_time.minute,
                        id=f"weekly_{scheduled_time.strftime('%w%H%M')}_{data['namaBlast']}_only",
                        replace_existing=True
                    )
                else:
                    print(f"⚠️ Jadwal {waktu_str} sudah lewat, skip.")
            return jsonify({"message": f"WA blast berhasil dijadwalkan untuk {len(periode_list)} waktu."}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 400


    def run_wa_blast_plulusan(self, data, insert=False):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            tahun_lulus = int(data['targetType2']) - 1  # 👈 cohort dikurangi 1 untuk dapat tahun lulus sebenarnya
            status_blast = data['StatusBlast2']
            tanggal_mulai = data['TanggalBlast2']
            tanggal_selesai = data['tanggalSelesaiBlast']
            nama_blast = data['namaBlast']
            periode = json.dumps(data['periodeBlast']) if isinstance(data['periodeBlast'], list) else data['periodeBlast']
            status = 'G'

            # 🔽 Baca template dari file YAML
            with open('template_wa_plulusan.yaml', 'r', encoding='utf-8') as f:
                template_data = yaml.safe_load(f)
            template_pesan = template_data.get('template_pesan', '')

            cursor.execute("""
                SELECT l.nama, l.noWa, l.nim, l.jenisKelamin, l.idProdi, l.tahunLulus, p.namaProdi 
                FROM lulusan l
                JOIN prodi p ON l.idProdi = p.idProdi
                WHERE l.tahunLulus = %s AND l.noWa IS NOT NULL AND l.noWa != ''
            """, (tahun_lulus,))
            users = cursor.fetchall()
            print(f"👥 Jumlah alumni ditemukan: {len(users)}")

            if insert:
                id_wablast = self.generate_id_wablast(cursor)
                print("📌 idWaBlast:", id_wablast)

                cursor.execute("""
                    INSERT INTO wablast 
                    (idWaBlast, namaBlast, targetType2, targetId2, TanggalBlast2, StatusBlast2, 
                    tanggalSelesaiBlast, isiPesan, periodeBlast)
                    VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s)
                """, (
                    id_wablast, nama_blast, tahun_lulus, tanggal_mulai, status_blast,
                    tanggal_selesai, template_pesan, periode
                ))
                print("✅ INSERT ke wablast berhasil")

                for user in users:
                    id_detail = self.generate_id_detail_wa(cursor)
                    print(f"➡️ INSERT detailwablast: {id_detail} untuk NIM: {user['nim']}")

                    cursor.execute("""
                        INSERT INTO detailwablast (idDetailWa, idWaBlast, statusPengiriman)
                        VALUES (%s, %s, %s)
                    """, (id_detail, id_wablast, status))

                    cursor.execute("""
                        UPDATE lulusan
                        SET idDetailWaPLulusan = %s
                        WHERE nim = %s
                    """, (id_detail, user['nim']))

                conn.commit()
                print("✅ COMMIT selesai")

                if status_blast == 'A':
                    current_hour = datetime.now().hour
                    if 4 <= current_hour < 12:
                        salam_waktu = "Selamat Pagi"
                    elif 12 <= current_hour < 15:
                        salam_waktu = "Selamat Siang"
                    elif 15 <= current_hour < 18:
                        salam_waktu = "Selamat Sore"
                    else:
                        salam_waktu = "Selamat Malam"

                    daftar_lulusan = ', '.join([u['nama'] for u in users])

                    for user in users:
                        jk = (user.get('jenisKelamin') or '').strip().upper()
                        panggilan = 'Saudara' if jk == 'L' else 'Saudari' if jk == 'P' else 'Saudara/Saudari'

                        cursor.execute("""
                            SELECT namaPerusahaan, alamatPerusahaan 
                            FROM penggunalulusan 
                            WHERE idPLulusan IN (
                                SELECT idPLulusan 
                                FROM lulusan_penggunalulusan_tablerelasi 
                                WHERE nim = %s
                            ) LIMIT 1
                        """, (user['nim'],))
                        psh = cursor.fetchone()
                        nama_psh = psh['namaPerusahaan'] if psh and psh.get('namaPerusahaan') else ''
                        lokasi_psh = psh['alamatPerusahaan'] if psh and psh.get('alamatPerusahaan') else ''

                        pesan_final = template_pesan \
                            .replace("{daftarLulusan}", daftar_lulusan) \
                            .replace("{NamaPsh}", nama_psh) \
                            .replace("{LokasiPsh}", lokasi_psh) \
                            .replace("{Panggilan}", panggilan) \
                            .replace("{Nama}", user['nama']) \
                            .replace("{salam_waktu}", salam_waktu)

                        pesan_final = re.sub(r"\{[^\}]+\}", "", pesan_final)
                        nomor = user['noWa']
                        chat_id = f"{nomor}@c.us"
                        self.send_wa(chat_id, pesan_final)
            else:
                print("⛔ Status Nonaktif, tidak kirim WA.")

            return jsonify({"message": "WA blast diproses."})
        except Exception as e:
            print("🔥 ERROR di run_wa_blast:", str(e))
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            conn.close()
    


            
    
    def send_monthly_wa_job_plulusan(self, data):
        print("📆 Eksekusi WA mingguan...")
        self.run_wa_reminder_plulusan(data)


    def run_wa_reminder_plulusan(self, data):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            idWaBlast = data['idWaBlast']

            # Ambil template dan status dari wablast
            cursor.execute("SELECT isiPesan, StatusBlast2 FROM wablast WHERE idWaBlast = %s", (idWaBlast,))
            result = cursor.fetchone()
            if not result:
                return jsonify({"error": "WA blast tidak ditemukan"}), 404

            raw_template = result['isiPesan']
            status_blast = result['StatusBlast2']

            if status_blast != 'A':
                print("⛔ WA Blast belum aktif. Reminder tidak dikirim.")
                return jsonify({"message": "WA blast belum aktif. Reminder tidak dikirim."}), 400

            # Ambil alumni yang belum mengisi
            cursor.execute("""
                SELECT l.nim, l.nama, l.jenisKelamin, l.noWa, l.tahunLulus, l.username, l.password, l.statusMengisi,
                    p.namaProdi
                FROM lulusan l
                JOIN prodi p ON l.idProdi = p.idProdi
                JOIN detailwablast d ON l.idDetailWa = d.idDetailWa
                WHERE d.idWaBlast = %s AND d.statusPengisian IS NULL
                AND l.noWa IS NOT NULL AND l.noWa != ''
            """, (idWaBlast,))
            alumni_rows = cursor.fetchall()

            current_hour = datetime.now().hour
            if 4 <= current_hour < 12:
                salam_waktu = "Selamat Pagi"
            elif 12 <= current_hour < 15:
                salam_waktu = "Selamat Siang"
            elif 15 <= current_hour < 18:
                salam_waktu = "Selamat Sore"
            else:
                salam_waktu = "Selamat Malam"

            import re
            for alumni in alumni_rows:
                if alumni.get('statusMengisi') == 'SG':
                    print(f"⏭ {alumni['noWa']} sudah mengisi, skip.")
                    continue

                no_wa = alumni['noWa']
                jk = (alumni.get('jenisKelamin') or '').strip().upper()
                panggilan = 'Saudara' if jk == 'L' else 'Saudari' if jk == 'P' else 'Saudara/Saudari'

                # Ambil perusahaan dan lokasi
                cursor.execute("""
                    SELECT namaPerusahaan, alamatPerusahaan 
                    FROM penggunalulusan 
                    WHERE idPLulusan IN (
                        SELECT idPLulusan 
                        FROM lulusan_penggunalulusan_tablerelasi 
                        WHERE nim = %s
                    ) LIMIT 1
                """, (alumni['nim'],))
                psh = cursor.fetchone()
                nama_psh = psh['namaPerusahaan'] if psh and psh.get('namaPerusahaan') else ''
                lokasi_psh = psh['alamatPerusahaan'] if psh and psh.get('alamatPerusahaan') else ''

                # ✅ Gunakan template dari isiPesan atau fallback
                template = raw_template.strip() if raw_template else (
                    ""
                )

                body = template \
                    .replace("{NamaPsh}", nama_psh) \
                    .replace("{LokasiPsh}", lokasi_psh) \
                    .replace("{Panggilan}", panggilan) \
                    .replace("{Nama}", alumni['nama']) \
                    .replace("{salam_waktu}", salam_waktu) \
                    .replace("{Prodi}", alumni.get('namaProdi', '')) \
                    .replace("{TahunLulus}", str(alumni.get('tahunLulus') or '')) \
                    .replace("{username}", alumni.get('username') or '') \
                    .replace("{password}", alumni.get('password') or '') \
                    .replace("{daftarLulusan}", alumni['nama'])  # Jika tetap ingin pakai, meskipun tunggal

                # Bersihkan placeholder sisa
                body = re.sub(r"\{[^\}]+\}", "", body)

                sent = self.send_wa(
                    chat_id=f"{no_wa}@c.us",
                    message=body
                )
                print(f"DEBUG: hasil send_wa untuk {no_wa} = {sent}")

            return jsonify({"message": f"Reminder dikirim ke {len(alumni_rows)} alumni yang belum mengisi."})

        except Exception as e:
            print("🔥 ERROR di run_wa_reminder:", str(e))
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()


    def aktifkan_wa_blast_plulusan(self, idWaBlast):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # Ambil data WA blast
            cursor.execute("SELECT * FROM wablast WHERE idWaBlast = %s", (idWaBlast,))
            wa_blast = cursor.fetchone()
            if not wa_blast:
                return jsonify({"error": "WA blast tidak ditemukan."}), 404

            now = datetime.now()

            # Ambil dan parsing periodeBlast
            periode_blast_raw = wa_blast.get('periodeBlast')
            periode_blast = None

            # Handle array json berisi banyak tanggal blast
            try:
                if isinstance(periode_blast_raw, str):
                    import json
                    decoded = json.loads(periode_blast_raw)
                    if isinstance(decoded, list):
                        # Ambil periodeBlast terawal
                        decoded = sorted(decoded)
                        if decoded:
                            periode_blast = datetime.strptime(decoded[0], '%Y-%m-%d %H:%M:%S')
                    else:
                        # fallback ke single datetime string lama
                        periode_blast = datetime.strptime(periode_blast_raw, '%Y-%m-%d %H:%M:%S')
            except Exception:
                return jsonify({"error": "Format periodeBlast tidak valid atau tidak bisa di-parse."}), 400

            if periode_blast and now < periode_blast:
                return jsonify({
                    "message": f"WA blast belum dapat diaktifkan karena belum waktunya. Jadwal aktif: {periode_blast.strftime('%Y-%m-%d %H:%M:%S')}."
                }), 200

            if wa_blast['StatusBlast2'] != 'A':
                tahun_lulus = wa_blast['targetType2']
                cursor.execute("""
                    SELECT l.nama, l.noWa, l.jenisKelamin, l.nim, l.tahunLulus, p.namaProdi
                    FROM lulusan l
                    JOIN prodi p ON l.idProdi = p.idProdi
                    WHERE l.tahunLulus = %s AND l.noWa IS NOT NULL AND l.noWa != ''
                """, (tahun_lulus,))
                users = cursor.fetchall()

                # Tentukan salam waktu
                jam_sekarang = datetime.now().hour
                if 4 <= jam_sekarang < 12:
                    salam_waktu = "Selamat Pagi"
                elif 12 <= jam_sekarang < 15:
                    salam_waktu = "Selamat Siang"
                elif 15 <= jam_sekarang < 18:
                    salam_waktu = "Selamat Sore"
                else:
                    salam_waktu = "Selamat Malam"

                import re
                pesan_logs = []

                # Ambil template dari database atau fallback
                raw_template = wa_blast.get('isiPesan')
                template_pesan = raw_template.strip() if raw_template else (
                   ""
                )

                for user in users:
                    nama = user['nama']
                    no_wa = user['noWa']
                    jenis_kelamin = (user['jenisKelamin'] or '').strip().upper()
                    tahun_lulus_user = user['tahunLulus']

                    if not nama or not no_wa:
                        pesan_logs.append(
                            f"127.0.0.1 - - [{datetime.now().strftime('%d/%b/%Y %H:%M:%S')}] ❌ INVALID DATA: {nama} / {no_wa}"
                        )
                        continue

                    sapaan = 'Saudara' if jenis_kelamin == 'L' else 'Saudari' if jenis_kelamin == 'P' else 'Saudara/Saudari'

                    # Ambil perusahaan dan lokasi
                    cursor.execute("""
                        SELECT namaPerusahaan, alamatPerusahaan 
                        FROM penggunalulusan 
                        WHERE idPLulusan IN (
                            SELECT idPLulusan 
                            FROM lulusan_penggunalulusan_tablerelasi 
                            WHERE nim = %s
                        ) LIMIT 1
                    """, (user['nim'],))
                    psh = cursor.fetchone()
                    nama_psh = psh['namaPerusahaan'] if psh and psh.get('namaPerusahaan') else ''
                    lokasi_psh = psh['alamatPerusahaan'] if psh and psh.get('alamatPerusahaan') else ''

                    pesan_final = template_pesan \
                        .replace("{daftarLulusan}", nama) \
                        .replace("{NamaPsh}", nama_psh) \
                        .replace("{LokasiPsh}", lokasi_psh) \
                        .replace("{Panggilan}", sapaan) \
                        .replace("{Nama}", nama) \
                        .replace("{salam_waktu}", salam_waktu)

                    # Hapus tag placeholder yang tidak terisi
                    pesan_final = re.sub(r"\{[^\}]+\}", "", pesan_final)

                    try:
                        chat_id = f"{no_wa}@c.us"
                        self.send_long_wa_plulusan(chat_id, pesan_final)
                        pesan_logs.append(
                            f"127.0.0.1 - - [{datetime.now().strftime('%d/%b/%Y %H:%M:%S')}] ✅ Terkirim ke {nama} (WA: {no_wa}, Lulus: {tahun_lulus_user})"
                        )
                    except Exception as e:
                        pesan_logs.append(
                            f"127.0.0.1 - - [{datetime.now().strftime('%d/%b/%Y %H:%M:%S')}] ❌ GAGAL ke {no_wa}: {str(e)}"
                        )

                # Update status blast
                cursor.execute("UPDATE wablast SET StatusBlast2 = 'A' WHERE idWaBlast = %s", (idWaBlast,))
                cursor.execute("UPDATE detailwablast SET statusPengiriman = 'S' WHERE idWaBlast = %s", (idWaBlast,))

                jumlah = len(users)

                cursor.execute("SELECT COUNT(*) AS total FROM wablastinglog")
                total = cursor.fetchone()['total'] + 1
                id_log = f"waglog{str(total).zfill(3)}"

                nama_blast = wa_blast['namaBlast']
                tanggal_mulai = wa_blast['TanggalBlast2']
                tanggal_selesai = wa_blast['tanggalSelesaiBlast']
                log_text = "\n".join(pesan_logs)

                cursor.execute("""
                    INSERT INTO wablastinglog (
                        idWaLog, idWaBlast, namaBlastWa, tanggalMulaiWa, tanggalSelesaiWa, jumlahWa, statusWa, pesanLoggingWa
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_log, idWaBlast, nama_blast, tanggal_mulai, tanggal_selesai, jumlah, 'S', log_text
                ))

                conn.commit()
                return jsonify({"message": "WA blast berhasil dikirim karena sudah waktunya."}), 200
            else:
                return jsonify({"message": "WA blast sudah aktif dan sebelumnya telah dikirim."}), 200

        except Exception as e:
            print("‼️ ERROR SERVER 500:")
            import traceback; traceback.print_exc()
            return jsonify({"error": str(e)}), 500

        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()

    def send_long_wa_plulusan(self, chat_id, message):
        MAX_CHUNK = 2000
        chunks = [message[i:i+MAX_CHUNK] for i in range(0, len(message), MAX_CHUNK)]
        for idx, chunk in enumerate(chunks, 1):
            self.send_wa_plulusan(chat_id, chunk)
            print(f"✅ Chunk {idx}/{len(chunks)} terkirim ke {chat_id}")
            time.sleep(1)  # Delay 1 detik antar chunk

    def send_wa_plulusan(self, chat_id, message):
        url = f"https://api.green-api.com/waInstance{ID_INSTANCE}/sendMessage/{API_TOKEN}"
        payload = {"chatId": chat_id, "message": message}
        headers = {'Content-Type': 'application/json'}

        if len(message) > 4000:
            print(f"⚠ Pesan ke {chat_id} terlalu panjang: {len(message)} karakter ➤ tidak dikirim")
            return False  # ⬅ Tambah return False supaya jelas gagal

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            try:
                result = response.json()
            except Exception:
                print(f"⚠ GAGAL PARSE JSON respon dari {chat_id} ➤ Status: {response.status_code} ➤ Body: {response.text}")
                return False  # ⬅ Tambah return False

            if response.status_code == 200 and "idMessage" in result:
                print(f"✅ BERHASIL ke {chat_id} ➤ ID: {result['idMessage']}")
                return True  # ⬅ Return True jika sukses
            else:
                print(f"⚠ GAGAL ke {chat_id} ➤ Status: {response.status_code} ➤ Respon: {result}")
                return False  # ⬅ Return False jika gagal status atau tidak ada idMessage

        except Exception as e:
            print(f"❌ ERROR kirim ke {chat_id}: {str(e)}")
            return False  # ⬅ Return False jika exception


    def nonaktifkan_wa_blast_plulusan(self, idWaBlast):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # Ambil data blast WA
            cursor.execute("SELECT * FROM wablast WHERE idWaBlast = %s", (idWaBlast,))
            blast = cursor.fetchone()

            if not blast:
                return jsonify({"error": "WA blast tidak ditemukan."}), 404

            if blast['StatusBlast2'] == 'N':
                return jsonify({"message": "WA blast sudah dalam status Nonaktif."}), 200

            # Update hanya status di wablast
            cursor.execute("UPDATE wablast SET StatusBlast2 = 'N' WHERE idWaBlast = %s", (idWaBlast,))
            conn.commit()

            # Batalkan semua job terjadwal berdasarkan periodeBlast
            if self.scheduler:
                nama_blast = blast['namaBlast']
                periode_str = blast['periodeBlast']

                try:
                    # Jika data string JSON list, misal '["2025-07-09 12:54:00", "2025-07-15 15:56:00"]'
                    periode_list = json.loads(periode_str)
                    if isinstance(periode_list, str):
                        periode_list = [periode_list]
                except Exception as e:
                    print(f"⚠️ Gagal parsing periodeBlast JSON: {e}")
                    # fallback: coba anggap string tunggal
                    periode_list = [periode_str]

                for waktu_str in periode_list:
                    try:
                        waktu = datetime.strptime(waktu_str, "%Y-%m-%d %H:%M:%S")
                        job_id = f"weekly_{waktu.strftime('%w%H%M')}_{nama_blast}_only"
                        self.scheduler.remove_job(job_id)
                        print(f"🛑 Scheduled job {job_id} dibatalkan.")
                    except Exception as e:
                        print(f"⚠️ Gagal membatalkan job untuk waktu {waktu_str}: {e}")

            return jsonify({"message": "WA blast berhasil dinonaktifkan dan semua jadwal dibatalkan."}), 200

        except Exception as e:
            print("‼️ ERROR saat nonaktifkan WA blast:")
            import traceback; traceback.print_exc()
            return jsonify({"error": str(e)}), 500

        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()


    def blast_wa_update_only_plulusan(self):
        data = request.json
        print("DATA DITERIMA UNTUK UPDATE WA BLAST:", data)
        if 'idWaBlast' not in data:
            return jsonify({"error": "idWaBlast wajib diisi untuk update"}), 400
        try:
            return self.update_wa_blast_plulusan(data)
        except Exception as e:
            return jsonify({"error": f"Gagal update WA blast: {str(e)}"}), 500

    def update_wa_blast_plulusan(self, data):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            nama_blast = data.get('namaBlast')
            tanggal_mulai = data.get('TanggalBlast2') or None
            tanggal_selesai = data.get('tanggalSelesaiBlast') or None
            isi_pesan = data.get('isiPesan')
            target_lulus = data.get('targetType2')
            periode_list = data.get('periodeBlast', [])
            id_wablast = data.get('idWaBlast')

            # Simpan periodeBlast sebagai string JSON list
            periode_json = json.dumps(periode_list)

            # Ambil data lama jika tanggal_mulai atau tanggal_selesai kosong/null
            cursor.execute("SELECT TanggalBlast2, tanggalSelesaiBlast FROM wablast WHERE idWaBlast = %s", (id_wablast,))
            existing = cursor.fetchone()
            if existing:
                if not tanggal_mulai:
                    tanggal_mulai = existing.get('TanggalBlast2')
                if not tanggal_selesai:
                    tanggal_selesai = existing.get('tanggalSelesaiBlast')

            cursor.execute("""
                UPDATE wablast SET 
                    namaBlast = %s,
                    TanggalBlast2 = %s,
                    tanggalSelesaiBlast = %s,
                    isiPesan = %s,
                    targetType2 = %s,
                    periodeBlast = %s
                WHERE idWaBlast = %s
            """, (
                nama_blast, tanggal_mulai, tanggal_selesai,
                isi_pesan, target_lulus, periode_json, id_wablast
            ))

            conn.commit()
            return jsonify({"message": "WA Blast berhasil di-update"}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def manual_wa_blast_plulusan(self, idWaBlast):
        try:
            selected_nims = request.json.get('selectedNIMs', [])
            if not selected_nims:
                return jsonify({"message": "Tidak ada alumni yang dipilih."}), 400

            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            waktu_mulai = datetime.now()

            cursor.execute("SELECT isiPesan FROM wablast WHERE idWaBlast = %s", (idWaBlast,))
            blast = cursor.fetchone()
            if not blast:
                return jsonify({"message": "WA blast tidak ditemukan"}), 404

            raw_template = blast.get('isiPesan')
            template = raw_template.strip() if raw_template else (
              ""
            )

            placeholders = ','.join(['%s'] * len(selected_nims))
            cursor.execute(f"""
                SELECT l.nim, l.nama, l.noWa, l.jenisKelamin, l.username, l.password, p.namaProdi, l.tahunLulus
                FROM lulusan l LEFT JOIN prodi p ON l.idProdi = p.idProdi
                WHERE l.nim IN ({placeholders})
            """, tuple(selected_nims))

            alumni_rows = cursor.fetchall()
            results = []
            success_log_entries = []
            log_messages = []

            for alumni in alumni_rows:
                no_wa = str(alumni.get('noWa') or '').strip()
                if not no_wa:
                    log_messages.append(
                        f"127.0.0.1 - - [{datetime.now().strftime('%d/%b/%Y %H:%M:%S')}] ❗ No WA tidak ditemukan untuk NIM {alumni['nim']}"
                    )
                    continue

                current_hour = datetime.now().hour
                salam_waktu = (
                    "Selamat Pagi" if 4 <= current_hour < 12 else
                    "Selamat Siang" if 12 <= current_hour < 15 else
                    "Selamat Sore" if 15 <= current_hour < 18 else
                    "Selamat Malam"
                )

                jk = (alumni.get('jenisKelamin') or '').strip().upper()
                namapanggilan = 'Saudara' if jk == 'L' else 'Saudari' if jk == 'P' else 'Saudara/Saudari'

                # Ambil info perusahaan pengguna lulusan
                cursor.execute("""
                    SELECT namaPerusahaan, alamatPerusahaan
                    FROM penggunalulusan 
                    WHERE idPLulusan IN (
                        SELECT idPLulusan 
                        FROM lulusan_penggunalulusan_tablerelasi 
                        WHERE nim = %s
                    )
                    LIMIT 1
                """, (alumni['nim'],))
                psh = cursor.fetchone()
                nama_psh = psh['namaPerusahaan'] if psh and psh.get('namaPerusahaan') else ''
                lokasi_psh = psh['alamatPerusahaan'] if psh and psh.get('alamatPerusahaan') else ''

                # Bangun isi pesan
                body = template \
                    .replace('{Panggilan}', namapanggilan) \
                    .replace('{Nama}', alumni.get('nama', '')) \
                    .replace('{username}', alumni.get('username', '')) \
                    .replace('{password}', alumni.get('password', '')) \
                    .replace('{Prodi}', alumni.get('namaProdi', '')) \
                    .replace('{TahunLulus}', str(alumni.get('tahunLulus', ''))) \
                    .replace('{salam_waktu}', salam_waktu) \
                    .replace('{daftarLulusan}', alumni.get('nama', '')) \
                    .replace('{NamaPsh}', nama_psh) \
                    .replace('{LokasiPsh}', lokasi_psh)

                # Hapus placeholder yang belum tergantikan
                import re
                body = re.sub(r"\{[^\}]+\}", "", body)

                sent_result = self.send_wa(chat_id=f"{no_wa}@c.us", message=body)
                sent = True if sent_result is True else False

                status = 'S' if sent else 'G'
                results.append({
                    "nim": alumni['nim'],
                    "nama": alumni['nama'],
                    "noWa": no_wa,
                    "status": status
                })

                log_time = datetime.now().strftime('%d/%b/%Y %H:%M:%S')

                if sent:
                    success_log_entries.append(
                        f"127.0.0.1 - - [{log_time}] Terkirim ke {alumni['nama']} (WA: {no_wa}, Lulus: {alumni.get('tahunLulus', '-')})"
                    )
                else:
                    log_messages.append(
                        f"127.0.0.1 - - [{log_time}] ❌ Gagal kirim ke {no_wa} (NIM {alumni['nim']})"
                    )

            waktu_selesai = datetime.now()

            # Generate ID Log baru
            cursor.execute("SELECT idWaLog FROM wablastinglog ORDER BY idWaLog DESC LIMIT 1")
            last_log = cursor.fetchone()
            import re
            if last_log:
                match = re.search(r'(\d+)$', last_log['idWaLog'])
                new_number = int(match.group(1)) + 1 if match else 1
            else:
                new_number = 1
            new_idLog = f"waglog{new_number:03d}"

            full_pesan = "\n".join(success_log_entries + log_messages)
            if not full_pesan.strip():
                full_pesan = "✅ Semua pesan WA terkirim sukses."

            status_log = 'S' if any(r['status'] == 'S' for r in results) else 'G'

            cursor.execute("""
                INSERT INTO wablastinglog 
                (idWaLog, idWaBlast, namaBlastWa, tanggalMulaiWa, tanggalSelesaiWa, jumlahWa, statusWa, pesanLoggingWa)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                new_idLog,
                idWaBlast,
                f"Manual WA Blast {waktu_mulai.strftime('%Y-%m-%d %H:%M:%S')}",
                waktu_mulai,
                waktu_selesai,
                len(results),
                status_log,
                full_pesan
            ))

            conn.commit()

            return jsonify({
                "message": "WA blast berhasil.",
                "results": results
            })

        except Exception as e:
            conn.rollback()
            print("❌ ERROR:", str(e))
            import traceback
            traceback.print_exc()
            return jsonify({"message": f"Server error: {str(e)}"}), 500

        finally:
            if cursor: cursor.close()
            if conn: conn.close()





