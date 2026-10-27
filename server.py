# server.py - Tudo em um só arquivo (Servidor + HTML)
# Rodar no Pydroid 3 e acessar http://127.0.0.1:8000
#
# Novidades desta versão:
#  - Salas otimizadas: envio simultâneo, histórico das últimas mensagens,
#    limite de pessoas, nomes repetidos viram "Nome (2)", anti-spam
#  - Mensagem de áudio (gravar, enviar e ouvir dentro da sala)
#  - Cliente reconecta sozinho e a lista de salas atualiza sozinha

import asyncio
import base64
import json
import time
import os
from collections import deque

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI(title="Mundo Chat")

# ==========================================
# CONFIGURAÇÕES (fáceis de mexer)
# ==========================================
HOST = "0.0.0.0"  # use "0.0.0.0" para outros aparelhos do Wi-Fi acessarem
PORT = int(os.environ.get("PORT", 8000))

SALAS = {
    "geral": "Geral",
    "jogos": "Jogos",
    "estudos": "Estudos",
}

MAX_POR_SALA = 50          # pessoas por sala
MAX_TEXTO = 300            # letras por mensagem
HISTORICO = 30             # últimas mensagens de texto mostradas ao entrar
MAX_AUDIO_B64 = 900_000    # tamanho máximo do áudio (≈ 650 KB, uns 60 s)
MIMES_AUDIO = {"audio/webm", "audio/ogg", "audio/mp4", "audio/mpeg"}

# ==========================================
# 1. O CÓDIGO DO SITE (HTML + JS)
# ==========================================
HTML_CODE = r"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mundo - Chat</title>
<style>
  * { box-sizing: border-box; }
  [hidden] { display: none !important; }
  body {
    margin: 0; font-family: system-ui, sans-serif;
    background: #1a1a2e; color: #eee;
    height: 100vh; height: 100dvh; display: flex; flex-direction: column;
  }
  .tela { display: none; flex: 1; flex-direction: column; padding: 16px; gap: 12px; overflow: hidden; min-height: 0; }
  .tela.ativa { display: flex; }
  h2 { margin: 0; }

  input[type=text] {
    padding: 12px; font-size: 16px; border-radius: 8px;
    border: 1px solid #444; background: #222; color: #fff;
  }
  .btn {
    padding: 12px 16px; font-size: 16px; border-radius: 8px;
    border: none; background: #4361ee; color: white; cursor: pointer;
  }
  .btn:hover { background: #3a56d4; }
  .btn.sec { background: #555; }
  .btn.sec:hover { background: #666; }
  #lista-salas { display: flex; flex-direction: column; gap: 12px; overflow-y: auto; }

  #topo { display: flex; gap: 8px; align-items: center; }
  #titulo { font-size: 18px; font-weight: bold; }
  #online { color: #9aa3b5; font-size: 14px; margin-left: auto; }

  #log {
    flex: 1; min-height: 0; overflow-y: auto; background: #222; border-radius: 8px;
    padding: 12px; display: flex; flex-direction: column; gap: 8px;
  }
  .msg { background: #2f2f4a; border-radius: 10px; padding: 8px 10px; max-width: 85%; align-self: flex-start; word-wrap: break-word; }
  .msg.meu { background: #2b3a8c; align-self: flex-end; }
  .quem { font-size: 12px; color: #aab; margin-bottom: 2px; }
  .sis { color: #8b95a7; font-size: 13px; align-self: center; text-align: center; }
  audio { width: 230px; max-width: 100%; margin-top: 4px; }

  #baixo, #gravando { display: flex; gap: 8px; align-items: center; }
  #entrada { flex: 1; min-width: 0; }
  #gravando { background: #3a1f28; border-radius: 8px; padding: 8px; }
  #tempo { font-variant-numeric: tabular-nums; margin-right: auto; }
  .ponto { width: 12px; height: 12px; border-radius: 50%; background: #ff4d4d; animation: pisca 1s infinite; }
  @keyframes pisca { 50% { opacity: 0.25; } }
</style>
</head>
<body>

<div id="tela-menu" class="tela ativa">
  <h2>Escolha um tema</h2>
  <input id="nome" type="text" placeholder="Seu nome" maxlength="20" autocomplete="off">
  <div id="lista-salas"></div>
</div>

<div id="tela-chat" class="tela">
  <div id="topo">
    <button class="btn sec" id="btn-voltar">Voltar</button>
    <div id="titulo"></div>
    <div id="online"></div>
  </div>
  <div id="log"></div>

  <div id="gravando" hidden>
    <span class="ponto"></span>
    <span id="tempo">0:00</span>
    <button class="btn sec" id="btn-cancelar">Cancelar</button>
    <button class="btn" id="btn-enviar-audio">Enviar áudio</button>
  </div>

  <div id="baixo">
    <input id="entrada" type="text" placeholder="Mensagem" maxlength="300" autocomplete="off">
    <button class="btn sec" id="btn-mic" title="Gravar áudio">🎤</button>
    <button class="btn" id="btn-enviar">Enviar</button>
  </div>
</div>

<script>
const BASE_WS = (location.protocol === "https:" ? "wss:" : "ws:") + "//" + location.host;
const MAX_AUDIO_S = 60;
const MAX_AUDIO_BYTES = 600 * 1024;
const MAX_MSGS_NA_TELA = 150;

const $ = (id) => document.getElementById(id);
const telaMenu = $("tela-menu"), telaChat = $("tela-chat");
const inputNome = $("nome"), listaSalas = $("lista-salas");
const tituloChat = $("titulo"), onlineEl = $("online");
const log = $("log"), entrada = $("entrada");
const barraBaixo = $("baixo"), barraGravando = $("gravando"), tempoEl = $("tempo");

let ws = null, salaAtual = null, meuNome = "";
let tentativas = 0, reconectarTimer = null;

try { inputNome.value = localStorage.getItem("nome") || ""; } catch (_) {}

/* ---------- MENU ---------- */
async function carregarSalas(silencioso) {
  if (!silencioso) listaSalas.textContent = "Carregando…";
  try {
    const resp = await fetch("/salas", { cache: "no-store" });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const salas = await resp.json();
    const botoes = salas.map((s) => {
      const b = document.createElement("button");
      b.className = "btn";
      b.textContent = s.nome + "  (" + s.online + " online)";
      b.onclick = () => entrarSala(s);
      return b;
    });
    listaSalas.replaceChildren(...botoes);
  } catch (_) {
    const p = document.createElement("p");
    p.textContent = "Não consegui falar com o servidor. Ele está ligado?";
    const b = document.createElement("button");
    b.className = "btn sec";
    b.textContent = "Tentar de novo";
    b.onclick = () => carregarSalas(false);
    listaSalas.replaceChildren(p, b);
  }
}
setInterval(() => { if (telaMenu.classList.contains("ativa")) carregarSalas(true); }, 5000);

/* ---------- ENTRAR / SAIR ---------- */
function entrarSala(sala) {
  try { localStorage.setItem("nome", inputNome.value.trim()); } catch (_) {}
  salaAtual = sala;
  tentativas = 0;
  tituloChat.textContent = sala.nome;
  onlineEl.textContent = "";
  log.textContent = "";
  mostrarTela("chat");
  conectar();
}

function conectar() {
  fecharWS();
  const nome = inputNome.value.trim() || "Anônimo";
  const w = new WebSocket(BASE_WS + "/ws/" + salaAtual.id + "?nome=" + encodeURIComponent(nome));
  ws = w;
  w.onopen = () => { tentativas = 0; };
  w.onmessage = (ev) => { try { tratar(JSON.parse(ev.data)); } catch (_) {} };
  w.onclose = (ev) => {
    if (ws !== w) return;            // fomos nós que fechamos
    ws = null;
    if (!salaAtual) return;
    if (ev.code === 4003 || ev.code === 4004) { escrever("[desconectado]"); return; }
    reconectar();
  };
}

function reconectar() {
  if (tentativas >= 5) { escrever("[sem conexão com o servidor]"); return; }
  tentativas++;
  escrever("[reconectando… " + tentativas + "/5]");
  reconectarTimer = setTimeout(() => { if (salaAtual) conectar(); }, tentativas * 1500);
}

function fecharWS() {
  clearTimeout(reconectarTimer);
  if (ws) {
    const antigo = ws;
    ws = null;
    try { antigo.close(); } catch (_) {}
  }
}

function sair() {
  if (rec && rec.state === "recording") pararGravacao(true);
  salaAtual = null;
  fecharWS();
  mostrarTela("menu");
  carregarSalas(false);
}

function mostrarTela(qual) {
  telaMenu.classList.toggle("ativa", qual === "menu");
  telaChat.classList.toggle("ativa", qual === "chat");
}

/* ---------- MENSAGENS ---------- */
function tratar(d) {
  switch (d.tipo) {
    case "estado":
      meuNome = d.voce;
      log.textContent = "";
      (d.historico || []).forEach(mostrarChat);
      escrever("Você entrou como " + d.voce + ".");
      atualizarOnline(d.jogadores.length);
      break;
    case "chat":   mostrarChat(d); break;
    case "audio":  mostrarAudio(d); break;
    case "entrou": escrever("* " + d.nome + " entrou"); atualizarOnline(d.online); break;
    case "saiu":   escrever("* " + d.nome + " saiu"); atualizarOnline(d.online); break;
    case "aviso":  escrever(d.texto); break;
  }
}

function atualizarOnline(n) { onlineEl.textContent = n + " online"; }

function adicionar(el) {
  const perto = log.scrollHeight - log.scrollTop - log.clientHeight < 120;
  log.appendChild(el);
  while (log.children.length > MAX_MSGS_NA_TELA) log.removeChild(log.firstChild);
  if (perto || el.classList.contains("meu")) log.scrollTop = log.scrollHeight;
}

function escrever(texto) {
  const d = document.createElement("div");
  d.className = "sis";
  d.textContent = texto;
  adicionar(d);
}

function criarMsg(m) {
  const div = document.createElement("div");
  div.className = "msg" + (m.nome === meuNome ? " meu" : "");
  const quem = document.createElement("div");
  quem.className = "quem";
  quem.textContent = m.nome + (m.hora ? "  " + m.hora : "");
  div.appendChild(quem);
  return div;
}

function mostrarChat(m) {
  const div = criarMsg(m);
  const corpo = document.createElement("div");
  corpo.textContent = m.texto;
  div.appendChild(corpo);
  adicionar(div);
}

function mostrarAudio(m) {
  const div = criarMsg(m);
  const a = document.createElement("audio");
  a.controls = true;
  a.preload = "metadata";
  a.src = "data:" + m.mime + ";base64," + m.dados;
  div.appendChild(a);
  adicionar(div);
}

function enviar() {
  const texto = entrada.value.trim();
  if (!texto || !ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({ tipo: "chat", texto: texto }));
  entrada.value = "";
  entrada.focus();
}

/* ---------- ÁUDIO ---------- */
let rec = null, trechos = [], cancelado = false, inicioGrav = 0, timerGrav = null;

function escolherMime() {
  const opcoes = ["audio/webm;codecs=opus", "audio/webm", "audio/mp4", "audio/ogg;codecs=opus"];
  return opcoes.find((m) => MediaRecorder.isTypeSupported(m)) || "";
}

async function iniciarGravacao() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  if (!navigator.mediaDevices || !window.MediaRecorder) {
    escrever("Gravação indisponível aqui. Abra por http://127.0.0.1:8000 (ou use https).");
    return;
  }
  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({ audio: true });
  } catch (_) {
    escrever("Permita o uso do microfone para gravar.");
    return;
  }
  const mime = escolherMime();
  const opcoes = { audioBitsPerSecond: 32000 };
  if (mime) opcoes.mimeType = mime;

  trechos = [];
  cancelado = false;
  rec = new MediaRecorder(stream, opcoes);
  rec.ondataavailable = (e) => { if (e.data && e.data.size) trechos.push(e.data); };
  rec.onstop = () => {
    stream.getTracks().forEach((t) => t.stop());
    clearInterval(timerGrav);
    barraGravando.hidden = true;
    barraBaixo.hidden = false;
    if (cancelado) return;
    const seg = Math.round((Date.now() - inicioGrav) / 1000);
    enviarAudio(new Blob(trechos, { type: rec.mimeType || mime || "audio/webm" }), seg);
  };

  rec.start();
  inicioGrav = Date.now();
  barraBaixo.hidden = true;
  barraGravando.hidden = false;
  tempoEl.textContent = "0:00";
  timerGrav = setInterval(() => {
    const s = Math.floor((Date.now() - inicioGrav) / 1000);
    tempoEl.textContent = Math.floor(s / 60) + ":" + String(s % 60).padStart(2, "0");
    if (s >= MAX_AUDIO_S) pararGravacao(false);
  }, 250);
}

function pararGravacao(cancelar) {
  cancelado = !!cancelar;
  if (rec && rec.state !== "inactive") rec.stop();
}

function enviarAudio(blob, seg) {
  if (blob.size > MAX_AUDIO_BYTES) { escrever("Áudio muito grande. Grave algo mais curto."); return; }
  const leitor = new FileReader();
  leitor.onload = () => {
    const dados = String(leitor.result).split(",")[1] || "";
    const mime = (blob.type || "audio/webm").split(";")[0];
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ tipo: "audio", mime: mime, dados: dados, seg: seg }));
    } else {
      escrever("Sem conexão. O áudio não foi enviado.");
    }
  };
  leitor.readAsDataURL(blob);
}

/* ---------- EVENTOS ---------- */
$("btn-voltar").onclick = sair;
$("btn-enviar").onclick = enviar;
$("btn-mic").onclick = iniciarGravacao;
$("btn-cancelar").onclick = () => pararGravacao(true);
$("btn-enviar-audio").onclick = () => pararGravacao(false);
entrada.addEventListener("keydown", (e) => { if (e.key === "Enter") enviar(); });

carregarSalas(false);
</script>
</body>
</html>
"""

# ==========================================
# 2. A LÓGICA DO SERVIDOR (SALAS E WEBSOCKET)
# ==========================================
def hora() -> str:
    return time.strftime("%H:%M")


def limpar_nome(nome: str) -> str:
    return " ".join(str(nome).split())[:20] or "Anônimo"


class Cliente:
    """Uma pessoa conectada. 'fichas' controla o anti-spam."""

    __slots__ = ("ws", "nome", "fichas", "ultima")

    def __init__(self, ws: WebSocket, nome: str):
        self.ws = ws
        self.nome = nome
        self.fichas = 8.0
        self.ultima = time.monotonic()

    def pode(self, custo: float) -> bool:
        agora = time.monotonic()
        self.fichas = min(8.0, self.fichas + (agora - self.ultima))  # 1 ficha por segundo
        self.ultima = agora
        if self.fichas >= custo:
            self.fichas -= custo
            return True
        return False


class Sala:
    def __init__(self, sala_id: str, nome: str):
        self.id = sala_id
        self.nome = nome
        self.clientes = set()
        self.historico = deque(maxlen=HISTORICO)

    @property
    def online(self) -> int:
        return len(self.clientes)

    def nomes(self) -> list:
        return [c.nome for c in self.clientes]

    def nome_livre(self, nome: str) -> str:
        usados = set(self.nomes())
        if nome not in usados:
            return nome
        n = 2
        while "%s (%d)" % (nome, n) in usados:
            n += 1
        return "%s (%d)" % (nome, n)

    async def _enviar_um(self, c: Cliente, texto: str):
        try:
            await asyncio.wait_for(c.ws.send_text(texto), timeout=5)
            return None
        except Exception:
            return c  # falhou: será removido da sala

    async def enviar(self, dados: dict, exceto=None):
        texto = json.dumps(dados, ensure_ascii=False)  # codifica só uma vez
        alvos = [c for c in self.clientes if c is not exceto]
        if not alvos:
            return
        falhas = await asyncio.gather(*(self._enviar_um(c, texto) for c in alvos))
        for c in falhas:
            if c is not None:
                self.clientes.discard(c)


salas = {sid: Sala(sid, nome) for sid, nome in SALAS.items()}


async def avisar(c: Cliente, texto: str):
    try:
        await c.ws.send_json({"tipo": "aviso", "texto": texto})
    except Exception:
        pass


async def tratar(sala: Sala, c: Cliente, d: dict):
    tipo = d.get("tipo")

    if tipo == "chat":
        texto = str(d.get("texto") or "").strip()[:MAX_TEXTO]
        if not texto:
            return
        if not c.pode(1):
            await avisar(c, "Calma! Mensagens demais.")
            return
        msg = {"tipo": "chat", "nome": c.nome, "texto": texto, "hora": hora()}
        sala.historico.append(msg)
        await sala.enviar(msg)

    elif tipo == "audio":
        mime = str(d.get("mime") or "")
        dados = d.get("dados")
        if mime not in MIMES_AUDIO or not isinstance(dados, str) or not dados or len(dados) > MAX_AUDIO_B64:
            await avisar(c, "Áudio inválido ou muito longo.")
            return
        if not c.pode(3):
            await avisar(c, "Calma! Espere um pouco para enviar outro áudio.")
            return
        try:
            base64.b64decode(dados, validate=True)
        except Exception:
            await avisar(c, "Áudio inválido.")
            return
        await sala.enviar({
            "tipo": "audio",
            "nome": c.nome,
            "mime": mime,
            "dados": dados,
            "hora": hora(),
        })
    # qualquer outro tipo é ignorado


@app.get("/")
async def raiz():
    return HTMLResponse(HTML_CODE)


@app.get("/salas")
async def listar_salas():
    return [{"id": s.id, "nome": s.nome, "online": s.online} for s in salas.values()]


@app.websocket("/ws/{sala_id}")
async def ws_sala(ws: WebSocket, sala_id: str, nome: str = "Anônimo"):
    sala = salas.get(sala_id)
    if not sala:
        await ws.close(code=4004)
        return

    await ws.accept()
    if sala.online >= MAX_POR_SALA:
        await ws.send_json({"tipo": "aviso", "texto": "Sala cheia. Tente outra."})
        await ws.close(code=4003)
        return

    cliente = Cliente(ws, sala.nome_livre(limpar_nome(nome)))
    sala.clientes.add(cliente)

    await ws.send_json({
        "tipo": "estado",
        "voce": cliente.nome,
        "jogadores": sala.nomes(),
        "historico": list(sala.historico),
    })
    await sala.enviar({"tipo": "entrou", "nome": cliente.nome, "online": sala.online}, exceto=cliente)

    try:
        while True:
            try:
                dados = json.loads(await ws.receive_text())
            except json.JSONDecodeError:
                continue
            if isinstance(dados, dict):
                await tratar(sala, cliente, dados)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        sala.clientes.discard(cliente)
        await sala.enviar({"tipo": "saiu", "nome": cliente.nome, "online": sala.online})


# ==========================================
# 3. INICIALIZAÇÃO NO PYDROID
# ==========================================
# ==========================================
# 3. INICIALIZAÇÃO ROBUSTA PARA PYDROID
# ==========================================
if __name__ == "__main__":
    import sys
    import traceback

    print("=" * 55, flush=True)
    print("  Mundo Chat - Servidor")
    print("  Acesse http://%s:%d" % (HOST, PORT))
    print("  Deixe o Pydroid aberto e a tela ligada.")
    print("=" * 55, flush=True)

    def rodar():
        config = uvicorn.Config(
            app,
            host=HOST,
            port=PORT,
            log_level="info",
            loop="asyncio",            # não tenta usar uvloop (não existe no Pydroid)
            lifespan="off",            # ignora startup/shutdown (evita handlers inúteis)
            ws_ping_interval=20,       # mantém os WebSockets vivos
            ws_ping_timeout=20,
            timeout_keep_alive=30,
            access_log=True,
        )
        server = uvicorn.Server(config)

        # ⚠ ESSA LINHA É O PULO DO GATO ⚠
        # Sem isso, quando o Android manda SIGTERM/SIGINT (app em segundo
        # plano, tela apagando), o uvicorn encerra o servidor sozinho.
        # Desativando o handler, o loop continua vivo.
        server.install_signal_handlers = lambda: None

        asyncio.run(server.serve())

    try:
        rodar()
    except KeyboardInterrupt:
        print("\n[servidor encerrado manualmente]")
    except SystemExit:
        pass
    except BaseException:
        # Mostra o erro REAL em vez de fechar silenciosamente
        print("\n" + "=" * 55)
        print("  O SERVIDOR CAIU. Detalhes abaixo:")
        print("=" * 55)
        traceback.print_exc()
        sys.stderr.flush()
    finally:
        # Sempre espera um Enter, para a janela do Pydroid não fechar
        # antes de você ler o erro.
        try:
            print("\nPressione Enter para fechar esta janela...", flush=True)
            input()
        except (EOFError, KeyboardInterrupt):
            pass


