<?php
/**
 * ZeroCereals API v3
 * Nomenclature v2: fonte, data_aggiornamento, stato normalizzato
 */

error_reporting(0);
ini_set('display_errors', 0);
ini_set('log_errors', 1);
ini_set('error_log', __DIR__ . '/php_errors.log');

header("Access-Control-Allow-Origin: *");
header("Access-Control-Allow-Methods: GET, POST, OPTIONS");
header("Access-Control-Allow-Headers: Content-Type, X-Import-Key");
header("Content-Type: application/json; charset=utf-8");

if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(200);
    exit();
}

define('DB_HOST',     'localhost');
define('DB_NAME',     'dhfmzeyo_zerocereals_kb');
define('DB_USER',     'dhfmzeyo_zc_user');
define('DB_PASS',     'KSTwS9,jO-TDA4!m');
define('IMPORT_KEY',  '831807adc8a88be29409ec9a5111dd792a55cb35268cb8820c4f7da99fbdc9e5');
define('DATA_DIR',    __DIR__ . '/data/');

// ─── STATI VALIDI ─────────────────────────────────────────────
const STATI_KB    = ['da_completare','stima','verificato','sperimentale'];
const STATI_DOE   = ['esplorativa','doe_formale','annullata'];

// ─── DB ───────────────────────────────────────────────────────
function get_db(): PDO {
    static $pdo = null;
    if ($pdo === null) {
        try {
            $pdo = new PDO(
                "mysql:host=".DB_HOST.";dbname=".DB_NAME.";charset=utf8mb4",
                DB_USER, DB_PASS,
                [
                    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    PDO::ATTR_EMULATE_PREPARES   => false,
                ]
            );
        } catch (PDOException $e) {
            http_response_code(500);
            echo json_encode(['error'=>'DB connection failed','detail'=>$e->getMessage()]);
            exit();
        }
    }
    return $pdo;
}

function ok(array $data=[]): void {
    echo json_encode(['status'=>'ok']+$data);
    exit();
}

function err(string $msg, int $code=400): void {
    http_response_code($code);
    echo json_encode(['status'=>'error','message'=>$msg]);
    exit();
}

function load_json(string $filename): array {
    $path = DATA_DIR.$filename;
    if (!file_exists($path)) return [];
    $data = json_decode(file_get_contents($path), true);
    return is_array($data) ? $data : [];
}

function upsert(PDO $pdo, string $table, array $row, string $pk): void {
    if (empty($row)||!isset($row[$pk])) return;
    $cols  = array_keys($row);
    $vals  = array_values($row);
    $ph    = implode(',', array_fill(0,count($cols),'?'));
    $upd   = implode(',', array_map(
        fn($c)=>"`$c`=VALUES(`$c`)",
        array_filter($cols, fn($c)=>$c!==$pk)
    ));
    if (!$upd) return;
    $pdo->prepare(
        "INSERT INTO `$table` (`".implode('`,`',$cols)."`) VALUES ($ph)
         ON DUPLICATE KEY UPDATE $upd"
    )->execute($vals);
}

// ─── IMPORT GENERICO ──────────────────────────────────────────
function import_table(
    PDO $pdo, string $file, string $table, string $pk,
    array $int_f=[], array $float_f=[], array $skip_f=[]
): int {
    $rows = load_json($file);
    if (empty($rows)) return 0;
    $count = 0;
    foreach ($rows as $r) {
        if (empty($r[$pk])) continue;
        $row = [];
        foreach ($r as $k=>$v) {
            if (in_array($k,$skip_f)) continue;
            if ($v===null||$v==='') continue;
            if (in_array($k,$int_f))   $row[$k]=(int)$v;
            elseif (in_array($k,$float_f)) $row[$k]=(float)$v;
            else $row[$k]=is_array($v)?json_encode($v):(string)$v;
        }
        upsert($pdo,$table,$row,$pk);
        $count++;
    }
    return $count;
}

// ─── ROUTER ───────────────────────────────────────────────────
$action = $_GET['action'] ?? $_POST['action'] ?? '';

switch ($action) {

    // ── HEALTH ────────────────────────────────────────────────
    case 'health':
        $pdo = get_db();
        ok([
            'db'          => 'connected',
            'schema'      => 'v2',
            'ingredienti' => (int)$pdo->query("SELECT COUNT(*) FROM ingredienti")->fetchColumn(),
            'additivi'    => (int)$pdo->query("SELECT COUNT(*) FROM additivi")->fetchColumn(),
            'matrici_reol'=> (int)$pdo->query("SELECT COUNT(*) FROM matrici_reol")->fetchColumn(),
            'prove_doe'   => (int)$pdo->query("SELECT COUNT(*) FROM prove_doe")->fetchColumn(),
            'timestamp'   => date('Y-m-d H:i:s'),
        ]);
        break;

    // ── IMPORT JSON → MYSQL ───────────────────────────────────
    case 'import':
        $key = $_SERVER['HTTP_X_IMPORT_KEY'] ?? ($_POST['key']??'');
        if ($key!==IMPORT_KEY) err('Unauthorized',401);

        $pdo = get_db();
        $res = [];

        // Ingredienti — nomenclatura v2: fonte, data_aggiornamento
        $res['ingredienti'] = import_table($pdo,'ingredienti.json','ingredienti','id',
            ['id','ig','ig_crudo','ig_cotto','assorbimento_idrico_pct',
             'temp_gel_c','fitati_post_attivazione_pct','attivato'],
            ['proteina_g','amido_g','carboidrati_g','amido_puro_g','lipidi_g',
             'fibra_g','fibra_solubile_g','fibra_insolubile_g','zuccheri_g',
             'kcal','acido_fitico_mg','amido_resistente_g','inulina_g',
             'beta_glucani_g','calcio_mg','ferro_mg','magnesio_mg',
             'potassio_mg','ph_nativo']
        );

        // IG per stato
        $rows = load_json('ig_per_stato.json');
        $n=0;
        foreach ($rows as $r) {
            if (empty($r['ingrediente_id'])||empty($r['stato_processo'])) continue;
            $pdo->prepare(
                "INSERT INTO ingredienti_ig_per_stato
                 (ingrediente_id,stato_processo,ig_valore,ig_fonte,note,data_inserimento)
                 VALUES (?,?,?,?,?,?)
                 ON DUPLICATE KEY UPDATE
                 ig_valore=VALUES(ig_valore),ig_fonte=VALUES(ig_fonte),note=VALUES(note)"
            )->execute([
                (int)$r['ingrediente_id'],$r['stato_processo'],
                isset($r['ig_valore'])?(int)$r['ig_valore']:null,
                $r['ig_fonte']??null,$r['note']??null,$r['data_inserimento']??null,
            ]);
            $n++;
        }
        $res['ig_per_stato']=$n;

        // Additivi — nomenclatura v2: fonte, data_aggiornamento
        $res['additivi'] = import_table($pdo,'additivi.json','additivi','id',
            ['ig'],
            ['dose_min_pct','dose_max_pct','dose_ottimale_pct']
        );

        // Blend
        $res['blend'] = import_table($pdo,'blend.json','blend','id',
            ['score_struttura','score_sapore','score_lievitazione',
             'score_shelflife','ig_stimato','idratazione_min','idratazione_max'],
            ['proteine_g']
        );

        // Matrici ingredienti × ingredienti
        foreach ([
            ['matrici_reol.json','matrici_reol','id_a','id_b'],
            ['matrici_chim.json','matrici_chim','id_a','id_b'],
            ['matrici_sens.json','matrici_sens','id_a','id_b'],
        ] as [$file,$table,$a,$b]) {
            $rows=load_json($file); $n=0;
            foreach ($rows as $r) {
                if (empty($r[$a])||empty($r[$b])) continue;
                $pdo->prepare(
                    "INSERT INTO `$table` (`$a`,`$b`,punteggio,note,fonte,stato,data)
                     VALUES (?,?,?,?,?,?,?)
                     ON DUPLICATE KEY UPDATE
                     punteggio=VALUES(punteggio),note=VALUES(note),fonte=VALUES(fonte)"
                )->execute([
                    (int)$r[$a],(int)$r[$b],
                    isset($r['punteggio'])?(int)$r['punteggio']:null,
                    $r['note']??null,$r['fonte']??null,
                    $r['stato']??'stima',$r['data']??null,
                ]);
                $n++;
            }
            $res[$table]=$n;
        }

        // Matrice additivi × ingredienti
        $rows=load_json('matrici_additivi.json'); $n=0;
        foreach ($rows as $r) {
            if (empty($r['additivo_id'])||empty($r['ingrediente_id'])) continue;
            $pdo->prepare(
                "INSERT INTO matrici_additivi_ingredienti
                 (additivo_id,ingrediente_id,tipo_interazione,punteggio,note,fonte,stato,data)
                 VALUES (?,?,?,?,?,?,?,?)
                 ON DUPLICATE KEY UPDATE
                 tipo_interazione=VALUES(tipo_interazione),
                 punteggio=VALUES(punteggio),note=VALUES(note),fonte=VALUES(fonte)"
            )->execute([
                $r['additivo_id'],(int)$r['ingrediente_id'],
                $r['tipo_interazione']??null,
                isset($r['punteggio'])?(int)$r['punteggio']:null,
                $r['note']??null,$r['fonte']??null,
                $r['stato']??'stima',$r['data']??null,
            ]);
            $n++;
        }
        $res['matrici_additivi']=$n;

        // Scheda operativa
        $res['scheda_operativa']=import_table($pdo,'scheda_operativa.json',
            'scheda_operativa','id');

        ok(['imported'=>$res,'timestamp'=>date('Y-m-d H:i:s')]);
        break;

    // ── SALVA PROVA DOE ───────────────────────────────────────
    case 'save_doe':
        if ($_SERVER['REQUEST_METHOD']!=='POST') err('POST required');
        $data=json_decode(file_get_contents('php://input'),true);
        if (!$data) err('Invalid JSON');
        if (empty($data['id'])) err('Campo id obbligatorio');

        // Valida stato
        $stato=$data['stato']??'esplorativa';
        if (!in_array($stato,STATI_DOE))
            err('Stato non valido. Valori ammessi: '.implode(', ',STATI_DOE));

        $pdo=get_db();
        $row=['id'=>$data['id'],'stato'=>$stato];

        foreach (['blend_id','operatore','luogo','tipo_impastatrice',
                  'modello_impastatrice','tipo_forno','modello_forno',
                  'tipo_cella_lievitazione','strumenti_misura','note_impasto',
                  'note_sensoriali','foto_url','note_generali','esito_complessivo'] as $f)
            if (isset($data[$f])&&$data[$f]!=='') $row[$f]=(string)$data[$f];

        foreach (['numero_prova','friction_factor','t_cottura_fase1_c',
                  'durata_fase1_min','t_cottura_fase2_c','durata_fase2_min',
                  't_cottura_fase3_c','durata_fase3_min','sapore_1_9',
                  'texture_1_9','croccantezza_1_9','retrogusto_1_9',
                  'soddisfazione_1_9','gommosita_1_9','durata_puntatura_min',
                  'durata_apretto_min'] as $f)
            if (isset($data[$f])&&$data[$f]!=='') $row[$f]=(int)$data[$f];

        foreach (['t_ambiente_c','ur_ambiente_pct','t_farine_c',
                  't_acqua_calcolata_c','t_acqua_usata_c','t_impasto_uscita_c',
                  'ph_impasto','idratazione_pct','ceci_pct','quinoa_pct',
                  'psyllium_pct','lm_pct','t_puntatura_c','ur_puntatura_pct',
                  'volume_aumento_puntatura_pct','t_apretto_c','ur_apretto_pct',
                  'volume_aumento_apretto_pct','t_interna_fine_c',
                  'volume_specifico_ml_g','durezza_mollica_n','colore_crosta_l',
                  'colore_crosta_a','colore_crosta_b','ph_mollica',
                  'aw_prodotto','umidita_pct'] as $f)
            if (isset($data[$f])&&$data[$f]!=='') $row[$f]=(float)$data[$f];

        foreach (['fitasi_usata','gox_usata'] as $f)
            if (isset($data[$f])) $row[$f]=$data[$f]?1:0;

        if (!empty($data['data'])) $row['data']=$data['data'];

        upsert($pdo,'prove_doe',$row,'id');
        ok(['id'=>$data['id'],'stato'=>$stato,'saved'=>true]);
        break;

    // ── GET PROVE ─────────────────────────────────────────────
    case 'get_prove':
        $pdo   = get_db();
        $limit = min((int)($_GET['limit']??50),200);
        $blend = $_GET['blend_id']??null;
        $stato = $_GET['stato']??null;

        $where=[]; $args=[];
        if ($blend) { $where[]="blend_id=?"; $args[]=$blend; }
        if ($stato && in_array($stato,STATI_DOE)) {
            $where[]="stato=?"; $args[]=$stato;
        }
        $wstr = $where ? "WHERE ".implode(' AND ',$where) : "";

        $stmt=$pdo->prepare(
            "SELECT * FROM prove_doe $wstr
             ORDER BY data DESC, numero_prova DESC LIMIT $limit"
        );
        $stmt->execute($args);
        ok(['prove'=>$stmt->fetchAll(),'count'=>$stmt->rowCount()]);
        break;

    // ── GET SINGOLA PROVA ─────────────────────────────────────
    case 'get_prova':
        $id=$_GET['id']??'';
        if (!$id) err('id required');
        $pdo=$get_db=get_db();
        $stmt=$pdo->prepare("SELECT * FROM prove_doe WHERE id=?");
        $stmt->execute([$id]);
        $row=$stmt->fetch();
        if (!$row) err('Prova non trovata',404);
        ok(['prova'=>$row]);
        break;

    default:
        err("Azione non riconosciuta: '$action'",400);
}
