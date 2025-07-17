from flask import request
from services.email_service import EmailService
from services.wa_service import WhatsAppBlastService

email_service_instance = None
wa_service_instance = None

def register_routes(app, scheduler):
    global email_service_instance
    global wa_service_instance
    email_service_instance = EmailService(scheduler)
    wa_service_instance = WhatsAppBlastService(scheduler)

    @app.route('/api/tahun-lulus', methods=['GET'])
    def get_tahun_lulus():
        return email_service_instance.get_tahun_lulus()

    @app.route('/api/email-blast', methods=['POST'])
    def blast_email():
        data = request.get_json()  # Ambil data dari request
        return email_service_instance.blast_email(data)  # Kirim ke method

    @app.route('/api/email-blast/<idEmailBlast>', methods=['GET'])
    def get_email_blast_by_id(idEmailBlast):
        return email_service_instance.get_email_blast_by_id(idEmailBlast)

    @app.route('/api/email-pesan/<idEmailBlast>', methods=['GET'])
    def get_email_blast_by_id_pesan(idEmailBlast):
        return email_service_instance.get_email_blast_by_id_pesan(idEmailBlast)

    @app.route('/api/email-blast-update', methods=['PUT'])
    def blast_email_update_only():
        return email_service_instance.blast_email_update_only()

    @app.route('/api/custom-pesan', methods=['PUT'])
    def blast_email_pesan():
        return email_service_instance.blast_email_pesan()

    @app.route('/api/aktifkan_email_blast/<idEmailBlast>', methods=['POST'])
    def aktifkan_email_blast(idEmailBlast):
        return email_service_instance.aktifkan_email_blast(idEmailBlast)


    @app.route('/api/nonaktifkan_email_blast/<idEmailBlast>', methods=['POST'])
    def nonaktifkan_email_blast(idEmailBlast):
        return email_service_instance.nonaktifkan_email_blast(idEmailBlast)
    
    @app.route('/api/manual-blast/<idEmailBlast>', methods=['POST'])
    def manual_blast(idEmailBlast):
        return email_service_instance.manual_blast(idEmailBlast)
    
    @app.route('/api/email-blast-pengguna-lulusan', methods=['POST'])
    def blast_email_plulusan():
        data = request.get_json()  # Ambil data dari request
        return email_service_instance.blast_email_plulusan(data)  # Kirim ke method
    
    @app.route('/api/tahun-pengguna-lulusan', methods=['GET'])
    def get_tahun_lulus_plulusan():
        return email_service_instance.get_tahun_lulus_plulusan()

    
    @app.route('/api/aktifkan_email_blast_plulusan/<idEmailBlast>', methods=['POST'])
    def aktifkan_email_blast_plulusan(idEmailBlast):
        return email_service_instance.aktifkan_email_blast_plulusan(idEmailBlast)
    
    @app.route('/api/nonaktifkan_email_blast-penggunalulusan/<idEmailBlast>', methods=['POST'])
    def nonaktifkan_email_blast_plulusan(idEmailBlast):
        return email_service_instance.nonaktifkan_email_blast_plulusan(idEmailBlast)
    
    @app.route('/api/custom-pesan-penggunalulusan', methods=['PUT'])
    def blast_email_pesan_plulusan():
        return email_service_instance.blast_email_pesan_plulusan()
    
    @app.route('/api/email-blast-update-penggunalulusan', methods=['PUT'])
    def blast_email_update_only_Plulusan():
        return email_service_instance.blast_email_update_only_Plulusan()
    
    @app.route('/api/blast_wa', methods=['POST'])
    def blast_wa():
        data = request.get_json()
        return wa_service_instance.blast_wa(data)
    
    @app.route('/api/tahun_lulus_wa', methods=['GET'])
    def get_tahun_lulus_wa():
        return wa_service_instance.get_tahun_lulus_wa()
    
    @app.route('/api/aktifkan_wa_blast/<idWaBlast>', methods=['POST'])
    def aktifkan_wa_blast(idWaBlast):
        return wa_service_instance.aktifkan_wa_blast(idWaBlast)
    
    @app.route('/api/nonaktifkan_wa_blast/<idWaBlast>', methods=['POST'])
    def nonaktifkan_wa_blast(idWaBlast):
        return wa_service_instance.nonaktifkan_wa_blast(idWaBlast)
    
    @app.route('/api/wa-blast/<idWaBlast>', methods=['GET'])
    def get_wa_blast_by_id(idWaBlast):
        return wa_service_instance.get_wa_blast_by_id(idWaBlast)
    
    @app.route('/api/wa-blast-update', methods=['PUT'])
    def blast_wa_update_only():
        return wa_service_instance.blast_wa_update_only()
    
    @app.route('/api/manual-blast-wa/<idWaBlast>', methods=['POST'])
    def manual_wa_blast(idWaBlast):
        return wa_service_instance.manual_wa_blast(idWaBlast)
    
    @app.route('/api/blast_wa_plulusan', methods=['POST'])
    def blast_wa_plulusan():
        data = request.get_json()
        return wa_service_instance.blast_wa_plulusan(data)
    
    @app.route('/api/aktifkan_wa_blast_plulusan/<idWaBlast>', methods=['POST'])
    def aktifkan_wa_blast_plulusan(idWaBlast):
        return wa_service_instance.aktifkan_wa_blast_plulusan(idWaBlast)
    
    @app.route('/api/nonaktifkan_wa_blast_plulusan/<idWaBlast>', methods=['POST'])
    def nonaktifkan_wa_blast_plulusan(idWaBlast):
        return wa_service_instance.nonaktifkan_wa_blast_plulusan(idWaBlast)
    
    @app.route('/api/wa-blast-update-plulusan', methods=['PUT'])
    def blast_wa_update_only_plulusan():
        return wa_service_instance.blast_wa_update_only_plulusan()
    
    @app.route('/api/manual-blast-wa-plulusan/<idWaBlast>', methods=['POST'])
    def manual_wa_blast_plulusan(idWaBlast):
        return wa_service_instance.manual_wa_blast_plulusan(idWaBlast)
    
    @app.route('/api/manual-blast-plulusan/<idEmailBlast>', methods=['POST'])
    def manual_blast_plulusan(idEmailBlast):
        return email_service_instance.manual_blast_plulusan(idEmailBlast) 
        
    @app.route('/api/tahun_plulus_wa', methods=['GET'])
    def get_tahun_lulus_wa_plulusa():
        return wa_service_instance.get_tahun_lulus_wa_plulusan()
    
    
    






